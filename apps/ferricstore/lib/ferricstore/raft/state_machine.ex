defmodule Ferricstore.Raft.StateMachine do
  @moduledoc """
  Ra state machine for a single FerricStore shard.

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

  HLC timestamps are piggybacked on Raft commands. Raft submit paths stamp each
  log entry with the leader's current HLC timestamp before submitting it to ra.
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

  The Raft log grows unbounded unless compacted. Every
  `:release_cursor_interval` applied commands (default: 200_000), `apply/3`
  emits `{:checkpoint, ra_index, state}` and `{:release_cursor, ra_index}`
  effects. This tells ra that all log entries up to `ra_index` are fully
  reflected in the given state checkpoint and can be safely truncated after
  the checkpoint is materialized.

  The interval is stored in the machine state at init time (from the config
  map or application env) so that `apply/3` remains deterministic -- it never
  reads runtime configuration.
  """

  @behaviour :ra_machine

  import Kernel, except: [apply: 3]
  import Bitwise

  require Logger

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Commands.HyperLogLog
  alias Ferricstore.Commands.Json
  alias Ferricstore.Raft.BlobCommand
  alias Ferricstore.Raft.PerfToggles
  alias Ferricstore.Flow
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.OrderedIndex, as: FlowIndex
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
  @sm_apply_state_key :sm_apply_state
  @sm_standalone_staged_key :sm_standalone_staged_apply
  @sm_force_async_flow_history_key :sm_force_async_flow_history
  @sm_force_sync_flow_history_key :sm_force_sync_flow_history
  @sm_pending_write_keys [
    :sm_pending_writes,
    :sm_pending_originals,
    :sm_pending_values,
    :sm_pending_lmdb_values,
    :sm_pending_lmdb_mirror_ops,
    :sm_pending_lmdb_mirror_after_flush,
    :sm_pending_lmdb_mirror_default_shard,
    :sm_pending_lmdb_mirror_tagged,
    :sm_pending_flow_history_projections,
    :sm_pending_fast_claim_index_rollbacks,
    :sm_pending_flow_index_originals,
    :sm_pending_flow_index_counts,
    :sm_pending_flow_native_ops,
    :sm_pending_zset_index_ops,
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
    sm_pending_lmdb_mirror_default_shard: nil,
    sm_pending_lmdb_mirror_tagged: false,
    sm_pending_flow_history_projections: [],
    sm_pending_fast_claim_index_rollbacks: [],
    sm_pending_flow_index_originals: %{},
    sm_pending_flow_index_counts: %{},
    sm_pending_flow_native_ops: [],
    sm_pending_zset_index_ops: [],
    sm_pending_prob_creates: [],
    sm_pending_has_delete: false,
    sm_pending_fast_put_batch: false,
    sm_pending_fast_delete_batch: false,
    sm_pending_fast_staged_put_batch: false
  ]

  defguardp valid_cold_location(file_id, offset, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
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
  # ra_machine callbacks
  # ---------------------------------------------------------------------------

  @doc """
  Initializes the state machine for a shard.

  The `config` map must include (v2 -- path-based, no NIF store reference):

    * `:shard_index` -- zero-based shard index
    * `:shard_data_path` -- absolute path to the shard's Bitcask data directory
    * `:active_file_id` -- numeric ID of the active log file
    * `:active_file_path` -- absolute path to the active log file
    * `:ets` -- ETS table name (already created)

  Optional:

    * `:release_cursor_interval` -- number of applies between release_cursor
      effects (default: #{@default_release_cursor_interval}). Can also be set
      via `Application.get_env(:ferricstore, :release_cursor_interval)`.

  Returns the initial machine state.
  """
  @impl true
  @spec init(map()) :: shard_state()
  def init(config) do
    data_dir =
      Map.get(
        config,
        :data_dir,
        Ferricstore.DataDir.root_from_shard_path(config.shard_data_path)
      )

    interval =
      Map.get_lazy(config, :release_cursor_interval, fn ->
        Application.get_env(
          :ferricstore,
          :release_cursor_interval,
          @default_release_cursor_interval
        )
      end)

    %{
      shard_index: config.shard_index,
      shard_data_path: config.shard_data_path,
      shard_data_path_expanded: Path.expand(config.shard_data_path),
      active_file_id: config.active_file_id,
      active_file_path: config.active_file_path,
      ets: config.ets,
      data_dir: data_dir,
      data_dir_expanded: Path.expand(data_dir),
      instance_ctx: Map.get(config, :instance_ctx),
      instance_name: Map.get(config, :instance_name, :default),
      blob_side_channel_threshold_bytes: Map.get(config, :blob_side_channel_threshold_bytes, 0),
      zset_score_index_name:
        Map.get(config, :zset_score_index_name) ||
          elem(
            ZSetIndex.table_names(Map.get(config, :instance_name, :default), config.shard_index),
            0
          ),
      zset_score_lookup_name:
        Map.get(config, :zset_score_lookup_name) ||
          elem(
            ZSetIndex.table_names(Map.get(config, :instance_name, :default), config.shard_index),
            1
          ),
      flow_index_name:
        Map.get(config, :flow_index_name) ||
          elem(
            FlowIndex.table_names(Map.get(config, :instance_name, :default), config.shard_index),
            0
          ),
      flow_lookup_name:
        Map.get(config, :flow_lookup_name) ||
          elem(
            FlowIndex.table_names(Map.get(config, :instance_name, :default), config.shard_index),
            1
          ),
      flow_lmdb_path:
        Map.get_lazy(config, :flow_lmdb_path, fn ->
          Ferricstore.Flow.LMDB.path(config.shard_data_path)
        end),
      flow_lmdb_mirror?:
        Map.get_lazy(config, :flow_lmdb_mirror?, &Ferricstore.Flow.LMDB.mirror?/0),
      active_file_size:
        Map.get_lazy(config, :active_file_size, fn ->
          file_size_or_zero(config.active_file_path)
        end),
      file_stats:
        Map.get_lazy(config, :file_stats, fn ->
          initial_file_stats(config.shard_data_path, config.ets, config.active_file_id)
        end),
      merge_config: Map.get(config, :merge_config, default_merge_config()),
      max_active_file_size:
        Map.get_lazy(config, :max_active_file_size, fn ->
          case Map.get(config, :instance_ctx) do
            %{max_active_file_size: max_file_size} ->
              max_file_size

            _ ->
              Application.get_env(
                :ferricstore,
                :max_active_file_size,
                @default_max_active_file_size
              )
          end
        end),
      flow_async_history: flow_async_history_config(config),
      applied_count: 0,
      release_cursor_interval: interval,
      pending_release_cursor_index: nil,
      pending_replay_safe_marker_index: nil,
      pending_release_cursor_checkpoint_indices: MapSet.new(),
      # When a node joins with pre-existing Bitcask data (from direct copy or
      # object storage snapshot), skip_below_index prevents re-applying entries
      # that are already in Bitcask + ETS. Entries at or below this index are
      # no-ops — the data was recovered from disk via recover_keydir.
      skip_below_index: Map.get(config, :skip_below_index, 0),
      # Cross-shard operation locks and intents — persisted in Raft state
      # so they survive shard restarts, snapshots, and leader failovers.
      cross_shard_locks: %{},
      cross_shard_intents: %{}
    }
  end

  @doc """
  Applies a replicated command to the shard state.

  Supported commands:

    * `{:put, key, value, expire_at_ms}` -- Write a key-value pair with optional
      expiry. Writes to Bitcask (sync NIF) and updates ETS.
    * `{:put_batch, entries}` -- Hot-path write-only SET batch where entries
      are `{key, value, expire_at_ms}` tuples. Stages Bitcask records, then
      publishes ETS after append succeeds. Returns `{:ok, results}`.
    * `{:delete, key}` -- Delete a key. Writes a tombstone to Bitcask, removes
      from ETS.
    * `{:delete_batch, keys}` -- Hot-path DEL batch. Returns `{:ok, results}`.
    * `{:delete_prefix, prefix}` -- Delete all keys matching a raw key prefix.
    * `{:batch, commands}` -- Apply a mixed list of commands atomically. Use
      this shape when later commands in the same Ra entry need pending
      read-your-own-write state. Returns `{:ok, results}` where results is a
      list of individual command results.
    * `{:list_op, key, operation}` -- Execute a list operation (LPUSH, RPUSH,
      LPOP, RPOP, etc.) as an atomic read-modify-write. Reads the current value
      from ETS/Bitcask, delegates to `ListOps.execute/4`, and persists the result.
    * `{:compound_put, compound_key, value, expire_at_ms}` -- Write a hash/set/zset
      field. Inserts `{compound_key, value, expire_at_ms}` into ETS and Bitcask.
    * `{:compound_delete, compound_key}` -- Delete a hash/set/zset field. Removes
      the compound key from ETS and Bitcask.
    * `{:compound_delete_prefix, prefix}` -- Delete all compound keys matching the
      given prefix from ETS and Bitcask. Used by DEL on data structures (hashes,
      sets, sorted sets) to clean up all fields.
    * `{:incr_float, key, delta}` -- Atomic read-modify-write float increment.
      Reads the current value, parses as float, adds `delta`, formats the result,
      and writes back. Returns `{:ok, new_float_string}` or
      `{:error, "ERR value is not a valid float"}`.
    * `{:append, key, suffix}` -- Atomic read-modify-write append. Reads the
      current value (or `""`), concatenates `suffix`, writes back. Returns
      `{:ok, byte_size(new_value)}`.
    * `{:getset, key, new_value}` -- Atomic get-and-set. Reads the old value,
      writes the new value with no expiry, returns the old value (or `nil`).
    * `{:getdel, key}` -- Atomic get-and-delete. Reads the value, deletes the
      key, returns the value (or `nil`).
    * `{:getex, key, expire_at_ms}` -- Atomic get-and-update-expiry. Reads the
      value, re-writes with the new `expire_at_ms`, returns the value (or `nil`).
    * `{:setrange, key, offset, value}` -- Atomic set-range. Reads the current
      value, pads with zero bytes if needed, replaces bytes at `offset`, writes
      back. Returns `{:ok, byte_size(new_value)}`.
    * `{:cas, key, expected, new_value, ttl_ms}` -- Compare-and-swap. Reads the
      current value; if it matches `expected`, writes `new_value` with optional
      TTL. Returns `1` (swapped), `0` (mismatch), or `nil` (key missing/expired).
    * `{:lock, key, owner, ttl_ms}` -- Distributed lock acquire. If the key does
      not exist, is expired, or is already held by the same owner, sets
      `{owner, ttl}`. Returns `:ok` or `{:error, reason}`.
    * `{:unlock, key, owner}` -- Distributed lock release. If the key exists and
      the owner matches, deletes the key. Returns `1` on success,
      `{:error, reason}` on owner mismatch.
    * `{:extend, key, owner, ttl_ms}` -- Distributed lock TTL extension. If the
      key exists and the owner matches, updates the TTL. Returns `1` on success,
      `{:error, reason}` on owner mismatch or missing key.
    * `{:ratelimit_add, key, window_ms, max, count}` -- Sliding window rate
      limiter. Reads counters, rotates windows, computes effective count, and
      updates. Returns `[status, count, remaining, ttl_ms]`.

  Returns `{new_state, result}` or `{new_state, result, effects}`.
  """
  # Skip entries that are already in Bitcask + ETS from a data sync copy.
  # When a node joins with pre-existing data (copied at raft_index N),
  # entries at or below N are no-ops — avoid redundant ETS overwrites
  # and Bitcask appends.
  @impl true
  def apply(%{index: idx} = meta, _command, %{skip_below_index: skip} = state)
      when skip > 0 and idx <= skip do
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}

    # Clear skip_below_index once we've passed it — no need to check on every apply
    new_state =
      if idx == skip, do: %{new_state | skip_below_index: 0}, else: new_state

    maybe_release_cursor(meta, old_count, new_state, :ok)
  end

  # Unwrap pre-serialized commands. ra stores {ttb, binary} in mem tables
  # when commands are pre-serialized in the Batcher. Deserialize back to
  # the original command tuple before dispatch.
  @impl true
  def apply(meta, {:ttb, binary}, state) when is_binary(binary) do
    __MODULE__.apply(meta, :erlang.binary_to_term(binary, [:safe]), state)
  end

  @impl true
  def apply(meta, {:async, _origin, {:release_cursor_poke, index}}, state)
      when is_integer(index) and index >= 0 do
    __MODULE__.apply(meta, {:release_cursor_poke, index}, state)
  end

  def apply(meta, {:async, {:release_cursor_poke, index}}, state)
      when is_integer(index) and index >= 0 do
    __MODULE__.apply(meta, {:release_cursor_poke, index}, state)
  end

  def apply(meta, {:release_cursor_poke, index}, state)
      when is_integer(index) and index >= 0 do
    with_apply_time(meta, fn ->
      maybe_release_cursor(meta, state.applied_count, state, :ok)
    end)
  end

  # Async commands. Router on the origin node has already persisted the write
  # Async single-command path. Delegates to apply_single which handles
  # origin-skip via the embedded origin node tag.
  @impl true
  def apply(meta, {:async, _origin, _inner_cmd} = cmd, state) do
    apply_pending_with_time(meta, state, fn -> apply_single(state, cmd) end)
  end

  # Backward-compat for 2-tuple async commands written by older binaries.
  # Treat as origin-unknown — apply unconditionally. Idempotent for put/delete,
  # may over-count repeated RMW on the same key (acceptable for one-time WAL
  # recovery; new writes use the 3-tuple form below).
  def apply(meta, {:async, _inner_cmd} = cmd, state) do
    apply_pending_with_time(meta, state, fn -> apply_single(state, cmd) end)
  end

  @impl true
  def apply(meta, {:put, key, value, expire_at_ms}, state) do
    with_apply_time(meta, fn ->
      redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

      result =
        case check_key_lock(state, redis_key, nil) do
          :ok ->
            with_pending_writes(state, fn -> do_put(state, key, value, expire_at_ms) end)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:put_blob_ref, key, encoded_ref, expire_at_ms}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_checked_put_blob_ref(state, key, encoded_ref, expire_at_ms)
    end)
  end

  def apply(meta, {:set, key, value, expire_at_ms, opts}, state) do
    with_apply_time(meta, fn ->
      redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

      result =
        case check_key_lock(state, redis_key, nil) do
          :ok ->
            with_pending_writes(state, fn -> do_set(state, key, value, expire_at_ms, opts) end)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:set_blob_ref, key, encoded_ref, expire_at_ms, opts}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_checked_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts)
    end)
  end

  def apply(meta, {:delete, key}, state) do
    with_apply_time(meta, fn ->
      redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

      result =
        case check_key_lock(state, redis_key, nil) do
          :ok ->
            with_pending_writes(state, fn -> do_delete(state, key) end)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:put_batch, entries}, state) when is_list(entries) do
    with_apply_time(meta, fn ->
      old_count = state.applied_count

      write_result =
        with_pending_writes(state, fn ->
          apply_put_batch_entries(state, entries)
        end)

      finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
    end)
  end

  def apply(meta, {:put_blob_batch, entries}, state) when is_list(entries) do
    with_apply_time(meta, fn ->
      old_count = state.applied_count

      write_result =
        with_pending_writes(state, fn ->
          apply_put_blob_batch_entries(state, entries)
        end)

      finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
    end)
  end

  def apply(meta, {:delete_batch, keys}, state) when is_list(keys) do
    with_apply_time(meta, fn ->
      old_count = state.applied_count

      write_result =
        with_pending_writes(state, fn ->
          apply_delete_batch_keys(state, keys)
        end)

      finish_hot_batch_apply(meta, old_count, state, length(keys), write_result)
    end)
  end

  def apply(meta, {:delete_prefix, prefix}, state) when is_binary(prefix) do
    with_apply_time(meta, fn ->
      result = with_pending_writes(state, fn -> do_delete_prefix(state, prefix) end)

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:batch, commands}, state) do
    with_apply_time(meta, fn ->
      commands = normalize_generic_batch_commands(commands)
      old_count = state.applied_count
      applied_increment = Enum.count(commands, &(not release_cursor_poke_command?(&1)))

      # All commands in a batch share one pending-writes buffer so they
      # are flushed in a single v2_append_batch_nosync NIF call.
      write_result =
        case prepare_apply_blob_command(state, {:batch, commands}) do
          {:ok, {:batch, prepared_commands}} ->
            with_pending_writes(state, fn ->
              Enum.map_reduce(prepared_commands, old_count, fn cmd, count ->
                result = apply_single(state, cmd)
                increment = if release_cursor_poke_command?(cmd), do: 0, else: 1
                {result, count + increment}
              end)
            end)

          {:ok, prepared_command} ->
            with_pending_writes(state, fn ->
              result = apply_single(state, prepared_command)
              {List.wrap(result), old_count + applied_increment}
            end)

          {:error, _reason} = error ->
            error
        end

      case write_result do
        {:error, _reason} = error ->
          new_state = %{state | applied_count: old_count + applied_increment}
          maybe_release_cursor(meta, old_count, new_state, error)

        {results, new_count} ->
          new_state = %{state | applied_count: new_count}
          maybe_release_cursor(meta, old_count, new_state, {:ok, results})
      end
    end)
  end

  def apply(meta, {:cross_shard_tx, shard_batches}, state) do
    with_apply_time(meta, fn ->
      old_count = state.applied_count

      write_result =
        with_cross_shard_pending_writes(state, fn ->
          ordered_entries = cross_shard_ordered_entries(shard_batches)

          Process.put(:tx_deleted_keys, MapSet.new())
          Process.put(:tx_pending_values, %{})

          try do
            case Enum.reduce_while(ordered_entries, {:ok, %{}, %{}}, fn
                   {_orig_idx, shard_idx, pos, entry, sandbox_namespace},
                   {:ok, results, stores} ->
                     {store, stores} =
                       case Map.fetch(stores, shard_idx) do
                         {:ok, cached} ->
                           {cached, stores}

                         :error ->
                           store = build_cross_shard_store(shard_idx, state)
                           {store, Map.put(stores, shard_idx, store)}
                       end

                     result = dispatch_cross_shard_entry(entry, sandbox_namespace, store, state)

                     case cross_shard_fatal_entry_error(entry, result) do
                       {:error, _reason} = error ->
                         {:halt, error}

                       nil ->
                         results =
                           Map.update(results, shard_idx, %{pos => result}, fn shard_results ->
                             Map.put(shard_results, pos, result)
                           end)

                         {:cont, {:ok, results, stores}}
                     end
                 end) do
              {:ok, results_by_position, _stores} ->
                cross_shard_results_by_batch_position(shard_batches, results_by_position)

              {:error, _reason} = error ->
                error
            end
          after
            Process.delete(:tx_deleted_keys)
            Process.delete(:tx_pending_values)
          end
        end)

      case write_result do
        {:error, _reason} = error ->
          new_state = %{state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, error)

        {:error, reason, flushed_state} ->
          new_state = %{flushed_state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, {:error, reason})

        {shard_results, flushed_state} ->
          new_state = %{flushed_state | applied_count: old_count + 1}
          maybe_release_cursor(meta, old_count, new_state, shard_results)
      end
    end)
  end

  # Legacy: list operations used to be sent as a single {:list_op} Raft entry
  # containing the entire operation. Now lists use compound keys (L:key\0pos)
  # and individual {:put}/{:delete} entries. This handler remains for WAL
  # replay of entries written before the compound-key migration.
  def apply(meta, {:list_op, key, operation}, state) do
    with_apply_time(meta, fn ->
      result = with_pending_writes(state, fn -> do_checked_list_op(state, key, operation) end)

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:list_op_lmove, src_key, dst_key, from_dir, to_dir}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_checked_lmove(state, src_key, dst_key, from_dir, to_dir)
    end)
  end

  def apply(meta, {:compound_put, compound_key, value, expire_at_ms}, state) do
    with_apply_time(meta, fn ->
      redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

      result =
        case check_key_lock(state, redis_key, nil) do
          :ok ->
            with_pending_writes(state, fn ->
              do_compound_put(state, redis_key, compound_key, value, expire_at_ms)
            end)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:compound_put_blob_ref, compound_key, encoded_ref, expire_at_ms}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_checked_compound_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms)
    end)
  end

  def apply(meta, {:compound_batch_put, redis_key, entries}, state)
      when is_binary(redis_key) and is_list(entries) do
    with_apply_time(meta, fn ->
      old_count = state.applied_count

      write_result =
        with_pending_writes(state, fn ->
          apply_compound_batch_put_entries(state, redis_key, entries)
        end)

      finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
    end)
  end

  def apply(meta, {:compound_blob_batch_put, redis_key, entries}, state)
      when is_binary(redis_key) and is_list(entries) do
    with_apply_time(meta, fn ->
      old_count = state.applied_count

      write_result =
        with_pending_writes(state, fn ->
          apply_compound_blob_batch_put_entries(state, redis_key, entries)
        end)

      finish_hot_batch_apply(meta, old_count, state, length(entries), write_result)
    end)
  end

  def apply(meta, {:compound_delete, compound_key}, state) do
    with_apply_time(meta, fn ->
      redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

      result =
        case check_key_lock(state, redis_key, nil) do
          :ok ->
            with_pending_writes(state, fn ->
              do_compound_delete(state, redis_key, compound_key)
            end)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:compound_batch_delete, redis_key, compound_keys}, state)
      when is_binary(redis_key) and is_list(compound_keys) do
    with_apply_time(meta, fn ->
      old_count = state.applied_count

      write_result =
        with_pending_writes(state, fn ->
          apply_compound_batch_delete_keys(state, redis_key, compound_keys)
        end)

      finish_hot_batch_apply(meta, old_count, state, length(compound_keys), write_result)
    end)
  end

  def apply(meta, {:compound_delete_prefix, prefix}, state) do
    with_apply_time(meta, fn ->
      redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

      result =
        case check_key_lock(state, redis_key, nil) do
          :ok ->
            with_pending_writes(state, fn ->
              do_compound_delete_prefix(state, redis_key, prefix)
            end)

          {:error, :key_locked} ->
            {:error, :key_locked}
        end

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:incr, key, delta}, state) do
    apply_pending_with_time(meta, state, fn -> do_incr(state, key, delta) end)
  end

  def apply(meta, {:incr_float, key, delta}, state) do
    apply_pending_with_time(meta, state, fn -> do_incr_float(state, key, delta) end)
  end

  def apply(meta, {:append, key, suffix}, state) do
    apply_pending_with_time(meta, state, fn -> do_append(state, key, suffix) end)
  end

  def apply(meta, {:append_blob_ref, key, encoded_ref}, state) do
    apply_pending_with_time(meta, state, fn -> do_append_blob_ref(state, key, encoded_ref) end)
  end

  def apply(meta, {:getset, key, new_value}, state) do
    apply_pending_with_time(meta, state, fn -> do_getset(state, key, new_value) end)
  end

  def apply(meta, {:getset_blob_ref, key, encoded_ref}, state) do
    apply_pending_with_time(meta, state, fn -> do_getset_blob_ref(state, key, encoded_ref) end)
  end

  def apply(meta, {:getdel, key}, state) do
    apply_pending_with_time(meta, state, fn -> do_getdel(state, key) end)
  end

  def apply(meta, {:getex, key, expire_at_ms}, state) do
    apply_pending_with_time(meta, state, fn -> do_getex(state, key, expire_at_ms) end)
  end

  def apply(meta, {:setrange, key, offset, value}, state) do
    apply_pending_with_time(meta, state, fn -> do_setrange(state, key, offset, value) end)
  end

  def apply(meta, {:setrange_blob_ref, key, offset, encoded_ref}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_setrange_blob_ref(state, key, offset, encoded_ref)
    end)
  end

  # Atomic SETBIT — read bitmap blob, mutate one bit, write back. Previously
  # the read+compute+write ran in the caller process (FerricStore.setbit/3),
  # losing updates under concurrent writes on the same key.
  def apply(meta, {:setbit, key, offset, bit_val}, state) do
    apply_pending_with_time(meta, state, fn -> do_setbit(state, key, offset, bit_val) end)
  end

  # Atomic HINCRBY / HINCRBYFLOAT — read compound field, add delta, write back.
  # Previously ran in caller process and lost updates under concurrent hincrby
  # on the same field.
  def apply(meta, {:hincrby, key, field, delta}, state) do
    apply_pending_with_time(meta, state, fn -> do_hincrby(state, key, field, delta) end)
  end

  def apply(meta, {:hincrbyfloat, key, field, delta}, state) do
    apply_pending_with_time(meta, state, fn -> do_hincrbyfloat(state, key, field, delta) end)
  end

  # Atomic ZINCRBY — read zset member's score, add increment, write back.
  # Also sets the type metadata atomically if absent (first write to the key).
  def apply(meta, {:zincrby, key, increment, member}, state) do
    apply_pending_with_time(meta, state, fn -> do_zincrby(state, key, increment, member) end)
  end

  def apply(meta, {:pfadd, key, elements}, state) do
    apply_pending_with_time(meta, state, fn ->
      HyperLogLog.handle_ast({:pfadd, [key | elements]}, build_string_value_store(state))
    end)
  end

  def apply(meta, {:pfmerge, dest_key, source_sketches}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_pfmerge(state, dest_key, source_sketches)
    end)
  end

  def apply(meta, {:pfmerge, dest_key, _source_keys, source_sketches}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_pfmerge(state, dest_key, source_sketches)
    end)
  end

  def apply(meta, {:json_set, key, path, value, flags}, state) do
    apply_pending_with_time(meta, state, fn ->
      Json.handle_ast({:json_set, key, path, value, flags}, build_string_value_store(state))
    end)
  end

  def apply(meta, {:json_del, key, path}, state) do
    apply_pending_with_time(meta, state, fn ->
      Json.handle_ast({:json_del, key, path}, build_string_value_store(state))
    end)
  end

  def apply(meta, {:json_numincrby, key, path, increment}, state) do
    apply_pending_with_time(meta, state, fn ->
      Json.handle_ast({:json_numincrby, key, path, increment}, build_string_value_store(state))
    end)
  end

  def apply(meta, {:json_arrappend, key, path, values}, state) do
    apply_pending_with_time(meta, state, fn ->
      Json.handle_ast({:json_arrappend, key, path, values}, build_string_value_store(state))
    end)
  end

  def apply(meta, {:json_toggle, key, path}, state) do
    apply_pending_with_time(meta, state, fn ->
      Json.handle_ast({:json_toggle, key, path}, build_string_value_store(state))
    end)
  end

  def apply(meta, {:json_clear, key, path}, state) do
    apply_pending_with_time(meta, state, fn ->
      Json.handle_ast({:json_clear, key, path}, build_string_value_store(state))
    end)
  end

  def apply(meta, {:spop, key, count}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_spop(state, key, count, Map.get(meta, :index, 0))
    end)
  end

  def apply(meta, {:zpop, key, count, direction}, state) do
    apply_pending_with_time(meta, state, fn -> do_zpop(state, key, count, direction) end)
  end

  def apply(meta, {:cas, key, expected, new_value, ttl_ms}, state) do
    apply_pending_with_time(meta, state, fn -> do_cas(state, key, expected, new_value, ttl_ms) end)
  end

  def apply(meta, {:cas_blob_ref, key, expected, encoded_ref, ttl_ms}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_cas_blob_ref(state, key, expected, encoded_ref, ttl_ms)
    end)
  end

  def apply(meta, {:lock, key, owner, ttl_ms}, state) do
    apply_pending_with_time(meta, state, fn -> do_lock(state, key, owner, ttl_ms) end)
  end

  def apply(meta, {:unlock, key, owner}, state) do
    apply_pending_with_time(meta, state, fn -> do_unlock(state, key, owner) end)
  end

  def apply(meta, {:extend, key, owner, ttl_ms}, state) do
    apply_pending_with_time(meta, state, fn -> do_extend(state, key, owner, ttl_ms) end)
  end

  def apply(meta, {:ratelimit_add, key, window_ms, max, count}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_ratelimit_add(state, key, window_ms, max, count, nil)
    end)
  end

  # Legacy 6-tuple variant: older submitters embedded now_ms before commands
  # were HLC-stamped at the Raft boundary. Stamped wrappers normalize this back
  # to the 5-tuple so the single log-entry timestamp wins.
  def apply(meta, {:ratelimit_add, key, window_ms, max, count, now_ms}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_ratelimit_add(state, key, window_ms, max, count, now_ms)
    end)
  end

  # ---------------------------------------------------------------------------
  # Cross-shard operation commands (mini-percolator)
  #
  # These commands support the CrossShardOp protocol: per-key locking through
  # Raft consensus, intent records for crash recovery, and locked writes.
  # ---------------------------------------------------------------------------

  def apply(meta, {:lock_keys, keys, owner_ref, expire_at_ms}, state) do
    with_apply_time(meta, fn ->
      {new_state, result} = do_lock_keys(state, keys, owner_ref, expire_at_ms)
      old_count = state.applied_count
      new_state = %{new_state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:unlock_keys, keys, owner_ref}, state) do
    apply_control_with_time(meta, state, fn -> do_unlock_keys(state, keys, owner_ref) end)
  end

  def apply(meta, {:cross_shard_intent, owner_ref, intent_map}, state) do
    apply_control_with_time(meta, state, fn -> do_write_intent(state, owner_ref, intent_map) end)
  end

  def apply(meta, {:delete_intent, owner_ref}, state) do
    apply_control_with_time(meta, state, fn -> do_delete_intent(state, owner_ref) end)
  end

  def apply(meta, {:get_intents}, state) do
    apply_control_with_time(meta, state, fn -> {state, do_get_intents(state)} end)
  end

  def apply(meta, {:get_lock_count}, state) do
    apply_control_with_time(meta, state, fn ->
      {state, map_size(Map.get(state, :cross_shard_locks, %{}))}
    end)
  end

  def apply(meta, {:clear_locks}, state) do
    apply_control_with_time(meta, state, fn ->
      {state |> Map.put(:cross_shard_locks, %{}) |> Map.put(:cross_shard_intents, %{}), :ok}
    end)
  end

  def apply(meta, {:locked_put, key, value, expire_at_ms, owner_ref}, state) do
    with_apply_time(meta, fn ->
      redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

      result =
        case check_key_lock(state, redis_key, owner_ref) do
          :ok ->
            with_pending_writes(state, fn -> do_put(state, key, value, expire_at_ms) end)

          {:error, _} = err ->
            err
        end

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:locked_put_blob_ref, key, encoded_ref, expire_at_ms, owner_ref}, state) do
    apply_pending_with_time(meta, state, fn ->
      do_locked_put_blob_ref(state, key, encoded_ref, expire_at_ms, owner_ref)
    end)
  end

  def apply(meta, {:locked_delete, key, owner_ref}, state) do
    with_apply_time(meta, fn ->
      redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

      result =
        case check_key_lock(state, redis_key, owner_ref) do
          :ok ->
            with_pending_writes(state, fn -> do_delete(state, key) end)

          {:error, _} = err ->
            err
        end

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  def apply(meta, {:locked_delete_prefix, prefix, owner_ref}, state) do
    with_apply_time(meta, fn ->
      redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

      result =
        case check_key_lock(state, redis_key, owner_ref) do
          :ok ->
            with_pending_writes(state, fn -> do_delete_prefix(state, prefix) end)

          {:error, _} = err ->
            err
        end

      old_count = state.applied_count
      new_state = %{state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  # ---------------------------------------------------------------------------
  # Probabilistic data structure commands (bloom, CMS, cuckoo, TopK)
  #
  # These commands replicate prob mutations through Raft so that followers
  # apply the same NIF writes to their local prob files. Read commands
  # (BF.EXISTS, CMS.QUERY, etc.) bypass Raft and go directly to the local
  # stateless pread NIF.
  # ---------------------------------------------------------------------------

  # -- Bloom --

  def apply(meta, {:bloom_create, key, num_bits, num_hashes, prob_meta}, state) do
    apply_prob_with_time(meta, state, fn ->
      create_bloom_metadata(state, key, num_bits, num_hashes, prob_meta)
    end)
  end

  def apply(meta, {:bloom_add, key, element, auto_create_params}, state) do
    apply_prob_with_time(meta, state, fn ->
      path = prob_path(state, key, "bloom")

      with :ok <- ensure_prob_dir(state) do
        case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
          :ok -> NIF.bloom_file_add(path, element)
          {:error, _reason} = error -> error
        end
      end
    end)
  end

  def apply(meta, {:bloom_madd, key, elements, auto_create_params}, state) do
    apply_prob_with_time(meta, state, fn ->
      path = prob_path(state, key, "bloom")

      with :ok <- ensure_prob_dir(state) do
        case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
          :ok -> NIF.bloom_file_madd(path, elements)
          {:error, _reason} = error -> error
        end
      end
    end)
  end

  # -- CMS --

  def apply(meta, {:cms_create, key, width, depth}, state) do
    apply_prob_with_time(meta, state, fn ->
      create_cms_metadata(state, key, width, depth)
    end)
  end

  def apply(meta, {:cms_incrby, key, items}, state) do
    apply_prob_with_time(meta, state, fn ->
      path = prob_path(state, key, "cms")
      NIF.cms_file_incrby(path, items)
    end)
  end

  def apply(meta, {:cms_merge, dst_key, src_keys, weights, create_params}, state) do
    apply_prob_with_time(meta, state, fn ->
      dst_path = prob_path(state, dst_key, "cms")
      src_paths = cms_source_paths(state, src_keys)

      with :ok <- ensure_prob_dir(state) do
        case maybe_create_cms_merge_dst(state, dst_path, dst_key, create_params) do
          :ok -> NIF.cms_file_merge(dst_path, src_paths, weights)
          {:error, _reason} = error -> error
        end
      end
    end)
  end

  # -- Cuckoo --

  def apply(meta, {:cuckoo_create, key, capacity, bucket_size}, state) do
    apply_prob_with_time(meta, state, fn ->
      create_cuckoo_metadata(state, key, capacity, bucket_size)
    end)
  end

  def apply(meta, {:cuckoo_add, key, element, auto_create_params}, state) do
    apply_prob_with_time(meta, state, fn ->
      path = prob_path(state, key, "cuckoo")

      with :ok <- ensure_prob_dir(state) do
        case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
          :ok -> NIF.cuckoo_file_add(path, element)
          {:error, _reason} = error -> error
        end
      end
    end)
  end

  def apply(meta, {:cuckoo_addnx, key, element, auto_create_params}, state) do
    apply_prob_with_time(meta, state, fn ->
      path = prob_path(state, key, "cuckoo")

      with :ok <- ensure_prob_dir(state) do
        case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
          :ok -> NIF.cuckoo_file_addnx(path, element)
          {:error, _reason} = error -> error
        end
      end
    end)
  end

  def apply(meta, {:cuckoo_del, key, element}, state) do
    apply_prob_with_time(meta, state, fn ->
      path = prob_path(state, key, "cuckoo")
      NIF.cuckoo_file_del(path, element)
    end)
  end

  # -- TopK --

  def apply(meta, {:topk_create, key, k, width, depth, decay}, state) do
    apply_prob_with_time(meta, state, fn ->
      create_topk_metadata(state, key, k, width, depth, decay)
    end)
  end

  def apply(meta, {:topk_add, key, elements}, state) do
    apply_prob_with_time(meta, state, fn ->
      path = prob_path(state, key, "topk")
      NIF.topk_file_add_v2(path, elements)
    end)
  end

  def apply(meta, {:topk_incrby, key, pairs}, state) do
    apply_prob_with_time(meta, state, fn ->
      path = prob_path(state, key, "topk")
      NIF.topk_file_incrby_v2(path, pairs)
    end)
  end

  # -- Flow --

  def apply(meta, {:flow_create, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_create(state, attrs) end)
  end

  def apply(meta, {:flow_create_many, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_create_many(state, attrs) end)
  end

  def apply(meta, {:flow_create_pipeline_batch, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_create_pipeline_batch(state, attrs) end)
  end

  def apply(meta, {:flow_named_value_put, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_named_value_put(state, attrs) end)
  end

  def apply(meta, {:flow_signal, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_signal(state, attrs) end)
  end

  def apply(meta, {:flow_spawn_children, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_spawn_children(state, attrs) end)
  end

  def apply(meta, {:flow_claim_due, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_claim_due(state, attrs) end)
  end

  def apply(meta, {:flow_extend_lease, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_extend_lease(state, attrs) end)
  end

  def apply(meta, {:flow_complete, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_complete(state, attrs) end)
  end

  def apply(meta, {:flow_complete_many, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_complete_many(state, attrs) end)
  end

  def apply(meta, {:flow_terminal_pipeline_batch, op, _key, attrs}, state)
      when op in [:complete, :retry, :fail, :cancel] and is_map(attrs) do
    apply_pending_with_time(meta, state, fn ->
      do_flow_terminal_pipeline_batch(state, op, attrs)
    end)
  end

  def apply(meta, {:flow_transition, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_transition(state, attrs) end)
  end

  def apply(meta, {:flow_transition_many, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_transition_many(state, attrs) end)
  end

  def apply(meta, {:flow_retry, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_retry(state, attrs) end)
  end

  def apply(meta, {:flow_retry_many, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_retry_many(state, attrs) end)
  end

  def apply(meta, {:flow_fail, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_fail(state, attrs) end)
  end

  def apply(meta, {:flow_fail_many, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_fail_many(state, attrs) end)
  end

  def apply(meta, {:flow_cancel, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_cancel(state, attrs) end)
  end

  def apply(meta, {:flow_cancel_many, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_cancel_many(state, attrs) end)
  end

  def apply(meta, {:flow_retention_cleanup, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_retention_cleanup(state, attrs) end)
  end

  def apply(meta, {:flow_rewind, _key, attrs}, state) when is_map(attrs) do
    apply_pending_with_time(meta, state, fn -> do_flow_rewind(state, attrs) end)
  end

  # ---------------------------------------------------------------------------
  # HLC-wrapped commands (spec 2G.6)
  #
  # Raft submit paths stamp commands before they enter the log. During apply,
  # the stamped physical HLC time is installed through `CommandTime`, so command
  # modules compute relative expiries and other time-derived values from the
  # same log-entry timestamp on every replica.
  # ---------------------------------------------------------------------------

  # Generic server command hook — allows server apps to replicate their own
  # commands through Raft without the library knowing what they are.
  # The server registers a raft_apply_hook callback on the Instance struct.
  def apply(meta, {:server_command, command}, state) do
    with_apply_time(meta, fn ->
      hook = raft_apply_hook(state)
      result = if hook, do: hook.(command), else: {:error, :no_hook}
      bump_applied(meta, state, result)
    end)
  end

  def apply(meta, {inner_command, %{hlc_ts: {physical_ms, _logical} = remote_ts}}, state)
      when is_tuple(inner_command) and is_integer(physical_ms) do
    merge_hlc(remote_ts)

    __MODULE__.apply(
      Map.put(meta, :system_time, physical_ms),
      normalize_stamped_command(inner_command),
      state
    )
  end

  # Catch-all: unknown commands should not crash the ra state machine.
  # Log the unrecognized command and return an error result so the caller
  # gets a meaningful error instead of ra crashing with FunctionClauseError.
  def apply(_meta, unknown_command, state) do
    require Logger
    Logger.error("StateMachine: unrecognized command: #{inspect(unknown_command)}")
    {state, {:error, {:unknown_command, unknown_command}}}
  end

  defp normalize_generic_batch_commands(commands) when is_list(commands) do
    Enum.flat_map(commands, &expand_generic_batch_command/1)
  end

  defp expand_generic_batch_command({:batch, commands}) when is_list(commands) do
    normalize_generic_batch_commands(commands)
  end

  defp expand_generic_batch_command({:put_batch, entries}) when is_list(entries) do
    Enum.map(entries, fn
      {key, value, expire_at_ms} -> {:put, key, value, expire_at_ms}
      invalid -> {:invalid_put_batch_entry, invalid}
    end)
  end

  defp expand_generic_batch_command({:put_blob_batch, entries}) when is_list(entries) do
    Enum.map(entries, fn
      {key, value, expire_at_ms, :value} ->
        {:put, key, value, expire_at_ms}

      {key, encoded_ref, expire_at_ms, :blob_ref} ->
        {:put_blob_ref, key, encoded_ref, expire_at_ms}

      invalid ->
        {:invalid_put_blob_batch_entry, invalid}
    end)
  end

  defp expand_generic_batch_command({:delete_batch, keys}) when is_list(keys) do
    Enum.map(keys, &{:delete, &1})
  end

  defp expand_generic_batch_command(command), do: [command]

  defp finish_hot_batch_apply(
         meta,
         old_count,
         state,
         applied_increment,
         {:error, _reason} = error
       ) do
    new_state = %{state | applied_count: old_count + applied_increment}
    maybe_release_cursor(meta, old_count, new_state, error)
  end

  defp finish_hot_batch_apply(meta, old_count, state, applied_increment, results) do
    new_state = %{state | applied_count: old_count + applied_increment}
    maybe_release_cursor(meta, old_count, new_state, {:ok, results})
  end

  @doc """
  Lifecycle hook called when the Raft node transitions roles.

  When becoming leader, generates a fresh HLC timestamp via `HLC.now/0` to
  ensure the leader's clock is up to date before it starts stamping commands.
  This is a side-effect only -- it does not affect the deterministic state
  machine output.

  In single-node mode, the node is always the leader. In multi-node clusters,
  this can be used to start/stop leader-only processes (e.g., merge scheduler,
  active expiry sweeper).

  Returns a list of effects (currently empty).
  """
  @impl true
  def state_enter(:leader, _state) do
    # Ensure the leader's HLC is freshly advanced. In multi-node clusters,
    # this guarantees the new leader's clock is at least at wall-clock time
    # before it begins stamping commands for followers to merge.
    HLC.now()
    []
  end

  def state_enter(:follower, _state), do: []
  def state_enter(:candidate, _state), do: []
  def state_enter(:await_condition, _state), do: []
  def state_enter(:delete_and_terminate, _state), do: []
  def state_enter(:receive_snapshot, _state), do: []
  def state_enter(_role, _state), do: []

  @doc """
  Periodic tick callback. Returns a list of effects (currently empty).
  """
  @impl true
  def tick(_time_ms, _state) do
    []
  end

  @doc """
  Initializes non-replicated auxiliary state.

  Aux state is local to each node and not replicated via Raft. Used for
  tracking hot-key statistics and other node-local metadata.
  """
  @impl true
  def init_aux(_name) do
    %{hot_keys: %{}}
  end

  @doc """
  Handles non-replicated auxiliary commands (5-arity new API).

  The `int_state` parameter is ra's internal state and must be passed back
  unchanged in the return tuple.

  Currently supports:
    * `{:cast, {:key_written, key}}` -- Increments a local hot-key counter.
  """
  # Cap hot_keys map to prevent unbounded memory growth. When the map exceeds
  # 10,000 entries, reset it to prevent the ra process heap from growing
  # indefinitely with unique keys. This bounds memory to ~1MB worst case.
  @hot_keys_max_size 10_000

  @impl true
  def handle_aux(_raft_state, :cast, {:key_written, key}, aux, int_state) do
    hot = aux.hot_keys

    if map_size(hot) >= @hot_keys_max_size do
      # Reset to prevent unbounded growth; start fresh with just this key.
      {:no_reply, %{aux | hot_keys: %{key => 1}}, int_state}
    else
      count = Map.get(hot, key, 0)
      {:no_reply, %{aux | hot_keys: Map.put(hot, key, count + 1)}, int_state}
    end
  end

  def handle_aux(_raft_state, _type, _cmd, aux, int_state) do
    {:no_reply, aux, int_state}
  end

  @doc """
  Returns a summary map for debugging and monitoring.

  Includes the shard index, ETS keydir size, total applied command count,
  and the release_cursor interval.
  """
  @impl true
  def overview(state) do
    ets_size =
      try do
        :ets.info(state.ets, :size)
      rescue
        ArgumentError -> 0
      end

    %{
      shard_index: state.shard_index,
      keydir_size: ets_size,
      applied_count: state.applied_count,
      release_cursor_interval: state.release_cursor_interval
    }
  end

  @doc false
  def __validate_pending_locations__(batch, locations) do
    validate_pending_locations(batch, locations)
  end

  @doc false
  def __apply_pending_locations_for_test__(state, file_id, batch, locations) do
    apply_pending_locations(state, file_id, batch, locations)
  end

  @doc false
  def apply_standalone_batch(commands, state) when is_list(commands) do
    commands = flatten_standalone_batch_commands(commands)

    apply_standalone(fn ->
      case commands do
        [{:cross_shard_tx, _shard_batches} = command] -> apply(%{}, command, state)
        _other -> apply(%{}, {:batch, commands}, state)
      end
    end)
  end

  defp flatten_standalone_batch_commands(commands) do
    Enum.flat_map(commands, fn
      {:batch, inner_commands} when is_list(inner_commands) -> inner_commands
      command -> [command]
    end)
  end

  @doc false
  def apply_standalone_command(command, state) do
    apply_standalone_command(command, %{}, state)
  end

  @doc false
  def apply_standalone_command(command, meta, state) when is_map(meta) do
    apply_standalone(fn -> apply(meta, command, state) end)
  end

  @doc false
  def apply_waraft_storage_command(command, meta, state) when is_map(meta) do
    with_sync_flow_history(fn -> apply(meta, command, state) end)
  end

  defp apply_standalone(fun) when is_function(fun, 0) do
    previous = Process.get(@sm_standalone_staged_key, :undefined)
    Process.put(@sm_standalone_staged_key, true)

    try do
      fun.()
    after
      case previous do
        :undefined -> Process.delete(@sm_standalone_staged_key)
        value -> Process.put(@sm_standalone_staged_key, value)
      end
    end
  end

  defp with_sync_flow_history(fun) when is_function(fun, 0) do
    previous = Process.get(@sm_force_sync_flow_history_key, :undefined)
    Process.put(@sm_force_sync_flow_history_key, true)

    try do
      fun.()
    after
      case previous do
        :undefined -> Process.delete(@sm_force_sync_flow_history_key)
        value -> Process.put(@sm_force_sync_flow_history_key, value)
      end
    end
  end

  @doc false
  def __compensate_cross_shard_partial_writes_for_test__(state, successful_groups, originals) do
    compensate_cross_shard_partial_writes(state, successful_groups, originals)
  end

  @doc false
  def __append_pending_batch_sync_for_test__(file_path, batch) do
    append_pending_batch_sync(file_path, batch, batch_contains_delete?(batch))
  end

  @doc false
  def __compensate_cross_shard_partial_writes_for_test__(
        state,
        successful_groups,
        originals,
        opts
      ) do
    if Keyword.get(opts, :standalone_staged?, false) do
      apply_standalone(fn ->
        compensate_cross_shard_partial_writes(state, successful_groups, originals)
      end)
    else
      compensate_cross_shard_partial_writes(state, successful_groups, originals)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: release_cursor compaction
  # ---------------------------------------------------------------------------

  # Checks whether the applied_count crossed an interval boundary AND the
  # ra meta contains a valid index. If both conditions are met, emits
  # checkpoint/release_cursor effects so ra can compact the log up to this
  # point.
  #
  # For single commands (put/delete), old_count + 1 == new applied_count,
  # so `div(old, interval) != div(new, interval)` is equivalent to
  # `rem(new, interval) == 0`.
  #
  # For batches, the applied_count may jump by N, potentially crossing one
  # or more interval boundaries. We emit a single release_cursor at the
  # batch's ra index when any boundary was crossed.
  #
  # When meta has no :index (e.g. unit tests calling apply/3 directly with
  # an empty map), the 2-tuple `{state, result}` is returned and no effect
  # is emitted.
  @spec maybe_release_cursor(map(), non_neg_integer(), shard_state(), term()) ::
          {shard_state(), term()} | {shard_state(), term(), list()}
  defp maybe_release_cursor(%{index: ra_index}, old_count, state, result) do
    state = consume_pending_state(state)
    checkpoint_clean_before_write? = apply_state_pop(:checkpoint_clean_before_write) == true
    release_cursor_blocked? = apply_state_pop(:release_cursor_blocked) == true

    checkpoint_dependencies_clean_before_write? =
      apply_state_pop(:checkpoint_dependencies_clean_before_write) == true

    dirty_checkpoint_indices = consume_checkpoint_dirty_indices()
    previous_pending_release_index = Map.get(state, :pending_release_cursor_index)

    previous_pending_checkpoint_indices =
      Map.get(state, :pending_release_cursor_checkpoint_indices, MapSet.new())

    record_cursor_metric(state, :last_applied_index, ra_index)

    # Wrap every reply with the ra_index it was applied at so the originating
    # Batcher can wait for the LOCAL state machine to also reach that index
    # before replying to the user. Otherwise a writer on a follower can see
    # the leader's :applied event (replied) before the local follower's state
    # machine has applied — read-your-write violation in cluster mode.
    wrapped_result = {:applied_at, ra_index, result}

    # Notify the local Batcher for this shard that ra_index was applied
    # *locally*. The `:local` send_msg option causes ra to fire this on every
    # node that has a local member (i.e., every voter), so each node's Batcher
    # tracks its OWN last_local_applied_idx independently.
    batcher_name = Ferricstore.Raft.Batcher.batcher_name(state.shard_index)
    notify_effect = {:send_msg, batcher_name, {:locally_applied, ra_index}, [:local]}

    interval = state.release_cursor_interval
    crossed_interval? = div(old_count, interval) != div(state.applied_count, interval)

    checkpoint_clean_now? =
      checkpoint_clean?(state) and checkpoint_indices_clean?(state, dirty_checkpoint_indices)

    {state, checkpoint_effects} =
      if crossed_interval? do
        checkpoint_state =
          state
          |> Map.put(:pending_release_cursor_index, ra_index)
          |> Map.put(
            :pending_release_cursor_checkpoint_indices,
            MapSet.union(previous_pending_checkpoint_indices, dirty_checkpoint_indices)
          )

        if checkpoint_clean_now? do
          {checkpoint_state, []}
        else
          {checkpoint_state, [{:checkpoint, ra_index, checkpoint_state}]}
        end
      else
        {state, []}
      end

    record_pending_checkpoint_count(state)

    pending_checkpoint_indices =
      Map.get(state, :pending_release_cursor_checkpoint_indices, MapSet.new())

    pending_checkpoint_clean? = checkpoint_indices_clean?(state, pending_checkpoint_indices)

    release_index =
      cond do
        checkpoint_clean_now? and pending_checkpoint_clean? ->
          Map.get(state, :pending_release_cursor_index)

        checkpoint_dependencies_clean_before_write? ->
          previous_pending_release_index

        checkpoint_clean_before_write? and pending_checkpoint_clean? ->
          previous_pending_release_index

        true ->
          nil
      end

    record_release_cursor_blocked_apply(state, release_cursor_blocked?)

    if release_cursor_blocked? do
      {state, wrapped_result, [notify_effect]}
    else
      {state, release_effects} =
        release_cursor_effects(
          state,
          release_index,
          ra_index,
          crossed_interval?,
          checkpoint_effects
        )

      {state, wrapped_result, [notify_effect | release_effects]}
    end
  end

  defp maybe_release_cursor(_meta, _old_count, state, result) do
    state = consume_pending_state(state)
    Process.delete(@sm_apply_state_key)

    # No meta (e.g. cross-shard sub-apply) — pass through untouched.
    {state, result}
  end

  defp release_cursor_effects(
         state,
         release_index,
         ra_index,
         crossed_interval?,
         checkpoint_effects
       )
       when is_integer(release_index) and release_index > 0 do
    if release_index > cursor_metric(state, :last_released_cursor_index) do
      case ensure_replay_safe_index(state, release_index) do
        {:ready, state} ->
          record_cursor_metric(state, :last_released_cursor_index, release_index)

          state =
            if Map.get(state, :pending_release_cursor_index) == release_index do
              state
              |> Map.put(:pending_release_cursor_index, nil)
              |> Map.put(:pending_replay_safe_marker_index, nil)
              |> Map.put(:pending_release_cursor_checkpoint_indices, MapSet.new())
            else
              state
            end

          record_pending_checkpoint_count(state)

          checkpoint_effects =
            if crossed_interval? and release_index == ra_index do
              [{:checkpoint, ra_index, state} | checkpoint_effects]
            else
              checkpoint_effects
            end

          {state, Enum.reverse(checkpoint_effects) ++ [{:release_cursor, release_index}]}

        {:pending, state} ->
          {state, checkpoint_effects}
      end
    else
      {state, checkpoint_effects}
    end
  end

  defp release_cursor_effects(
         state,
         _release_index,
         _ra_index,
         _crossed_interval?,
         checkpoint_effects
       ),
       do: {state, checkpoint_effects}

  defp ensure_replay_safe_index(state, release_index) do
    instance_ctx = checkpoint_ctx_for_state(state)

    bitcask_ready? =
      Ferricstore.Raft.ReplaySafeIndexWriter.durable?(
        instance_ctx,
        state.shard_index,
        state.shard_data_path,
        release_index
      )

    lmdb_ready? = flow_lmdb_replay_safe?(state, instance_ctx, release_index)
    history_ready? = flow_history_projector_replay_safe?(state, instance_ctx, release_index)

    if bitcask_ready? and lmdb_ready? and history_ready? do
      {:ready, state}
    else
      case request_replay_safe_indexes(
             state,
             instance_ctx,
             release_index,
             bitcask_ready?,
             lmdb_ready?,
             history_ready?
           ) do
        {:ready, state} -> {:ready, state}
        state -> {:pending, state}
      end
    end
  end

  defp flow_lmdb_replay_safe?(state, instance_ctx, release_index) do
    Ferricstore.Flow.LMDBWriter.durable?(
      instance_ctx,
      state.shard_index,
      state.shard_data_path,
      release_index
    )
  end

  defp flow_history_projector_replay_safe?(state, instance_ctx, release_index) do
    not flow_history_projector_required?(state) or
      HistoryProjector.durable?(
        instance_ctx,
        state.shard_index,
        state.shard_data_path,
        release_index
      )
  end

  defp flow_history_projector_required?(state) do
    flow_async_history?(state) or flow_claim_async_history?(state)
  end

  defp flow_claim_async_history?(state),
    do: Map.get(state, :flow_claim_async_history, true) == true

  defp request_replay_safe_indexes(
         state,
         instance_ctx,
         release_index,
         bitcask_ready?,
         lmdb_ready?,
         history_ready?
       ) do
    bitcask_status =
      cond do
        bitcask_ready? ->
          :durable

        Map.get(state, :pending_replay_safe_marker_index) == release_index ->
          :requested

        true ->
          Ferricstore.Raft.ReplaySafeIndexWriter.request(
            instance_ctx,
            state.shard_index,
            state.shard_data_path,
            release_index
          )
      end

    lmdb_status =
      cond do
        lmdb_ready? ->
          :durable

        true ->
          Ferricstore.Flow.LMDBWriter.request(
            instance_ctx,
            state.shard_index,
            state.shard_data_path,
            release_index
          )
      end

    history_status =
      cond do
        history_ready? ->
          :durable

        true ->
          HistoryProjector.request(
            instance_ctx,
            state.shard_index,
            state.shard_data_path,
            release_index
          )
      end

    if bitcask_status == :durable and lmdb_status == :durable and history_status == :durable do
      {:ready, Map.put(state, :pending_replay_safe_marker_index, nil)}
    else
      Map.put(state, :pending_replay_safe_marker_index, release_index)
    end
  end

  defp block_release_cursor_for_apply do
    apply_state_put(:release_cursor_blocked, true)
  end

  defp record_release_cursor_blocked_apply(state, true) do
    count = cursor_metric(state, :release_cursor_blocked_apply_count) + 1
    record_cursor_metric(state, :release_cursor_blocked_apply_count, count)

    :telemetry.execute(
      [:ferricstore, :raft, :release_cursor, :blocked],
      %{count: 1, consecutive_count: count},
      %{shard_index: Map.get(state, :shard_index)}
    )
  rescue
    _ -> :ok
  end

  defp record_release_cursor_blocked_apply(state, false) do
    if cursor_metric(state, :release_cursor_blocked_apply_count) != 0 do
      record_cursor_metric(state, :release_cursor_blocked_apply_count, 0)
    end
  rescue
    _ -> :ok
  end

  defp consume_checkpoint_dirty_indices do
    case apply_state_pop(:checkpoint_dirty_indices) do
      %MapSet{} = indices -> indices
      indices when is_list(indices) -> MapSet.new(indices)
      _ -> MapSet.new()
    end
  end

  defp record_checkpoint_dirty_index(shard_index) when is_integer(shard_index) do
    indices = apply_state_get(:checkpoint_dirty_indices, MapSet.new())
    apply_state_put(:checkpoint_dirty_indices, MapSet.put(indices, shard_index))
  end

  defp record_checkpoint_dirty_index(_shard_index), do: :ok

  defp remember_checkpoint_dependencies_clean_before_write(state) do
    if apply_state_get(:checkpoint_dependencies_clean_before_write) != true do
      indices = Map.get(state, :pending_release_cursor_checkpoint_indices, MapSet.new())

      if Enum.any?(indices) and checkpoint_indices_clean?(state, indices) do
        apply_state_put(:checkpoint_dependencies_clean_before_write, true)
      end
    end
  rescue
    _ -> :ok
  end

  defp checkpoint_indices_clean?(state, indices) do
    Enum.all?(indices, fn shard_index -> checkpoint_index_clean?(state, shard_index) end)
  end

  defp checkpoint_index_clean?(state, shard_index) do
    case checkpoint_ctx_for_state(state) do
      nil ->
        true

      ctx ->
        flag_idx = shard_index + 1

        checkpoint_ref_clean?(Map.get(ctx, :checkpoint_flags), flag_idx) and
          checkpoint_ref_clean?(Map.get(ctx, :checkpoint_in_flight), flag_idx)
    end
  rescue
    _ -> false
  end

  defp checkpoint_ref_clean?(nil, _flag_idx), do: true

  defp checkpoint_ref_clean?(ref, flag_idx) do
    flag_idx > :atomics.info(ref).size or :atomics.get(ref, flag_idx) == 0
  end

  defp consume_pending_state(state) do
    case apply_state_pop(:pending_state) do
      nil ->
        state

      pending_state ->
        %{
          state
          | active_file_id: pending_state.active_file_id,
            active_file_path: pending_state.active_file_path,
            active_file_size: pending_state.active_file_size,
            file_stats: pending_state.file_stats
        }
    end
  end

  defp apply_state_get(field, default \\ nil) do
    @sm_apply_state_key
    |> Process.get(%{})
    |> Map.get(field, default)
  end

  defp apply_state_put(field, value) do
    state = Process.get(@sm_apply_state_key, %{})
    Process.put(@sm_apply_state_key, Map.put(state, field, value))
  end

  defp apply_state_pop(field, default \\ nil) do
    state = Process.get(@sm_apply_state_key, %{})
    {value, state} = Map.pop(state, field, default)

    if map_size(state) == 0 do
      Process.delete(@sm_apply_state_key)
    else
      Process.put(@sm_apply_state_key, state)
    end

    value
  end

  defp record_cursor_metric(%{shard_index: shard_index} = state, field, index)
       when is_atom(field) and is_integer(index) and index >= 0 do
    instance_ctx =
      case Map.get(state, :instance_ctx) do
        ctx when is_map(ctx) -> ctx
        _ -> instance_ctx_by_name(Map.get(state, :instance_name, :default))
      end

    case Map.get(instance_ctx, field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size
        if shard_index < size, do: :atomics.put(ref, shard_index + 1, index)

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp record_cursor_metric(_state, _field, _index), do: :ok

  defp record_pending_checkpoint_count(state) do
    count =
      state
      |> Map.get(:pending_release_cursor_checkpoint_indices, MapSet.new())
      |> MapSet.size()

    record_cursor_metric(state, :pending_release_cursor_checkpoint_count, count)
  rescue
    _ -> :ok
  end

  defp cursor_metric(%{shard_index: shard_index} = state, field) when is_atom(field) do
    case checkpoint_ctx_for_state(state) |> metric_ref(field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size
        if shard_index < size, do: :atomics.get(ref, shard_index + 1), else: 0

      _ ->
        0
    end
  rescue
    _ -> 0
  end

  defp cursor_metric(_state, _field), do: 0

  defp metric_ref(nil, _field), do: nil
  defp metric_ref(ctx, field), do: Map.get(ctx, field)

  # A release_cursor lets ra compact log entries. For the Bitcask-backed state
  # machine, the log must not be released past writes that are still only in
  # the OS page cache; the checkpoint flag is cleared only after fsync succeeds.
  defp checkpoint_clean?(
         %{instance_ctx: nil, instance_name: name, shard_index: shard_index} = state
       )
       when is_atom(name) do
    case checkpoint_ctx_for_state(state) do
      %FerricStore.Instance{} = instance_ctx ->
        checkpoint_clean?(%{instance_ctx: instance_ctx, shard_index: shard_index})

      _ ->
        name == :default
    end
  end

  # Only legacy/default state-machine callers may release without an Instance.
  # Any unresolved custom or malformed state fails closed because checkpoint
  # atomics are instance-owned and releasing Ra early can discard the only
  # durable copy of un-fsynced Bitcask writes.
  defp checkpoint_clean?(%{instance_ctx: nil, instance_name: :default}), do: true
  defp checkpoint_clean?(%{instance_ctx: nil}), do: false

  defp checkpoint_clean?(%{instance_ctx: instance_ctx, shard_index: shard_index})
       when is_map(instance_ctx) do
    flag_idx = shard_index + 1

    checkpoint_flag_clean? =
      case Map.get(instance_ctx, :checkpoint_flags) do
        nil ->
          true

        checkpoint_flags ->
          flag_idx > :atomics.info(checkpoint_flags).size or
            :atomics.get(checkpoint_flags, flag_idx) == 0
      end

    checkpoint_idle? =
      case Map.get(instance_ctx, :checkpoint_in_flight) do
        nil ->
          true

        checkpoint_in_flight ->
          flag_idx > :atomics.info(checkpoint_in_flight).size or
            :atomics.get(checkpoint_in_flight, flag_idx) == 0
      end

    checkpoint_flag_clean? and checkpoint_idle?
  end

  defp checkpoint_clean?(_state), do: true

  defp with_apply_time(%{system_time: now_ms}, fun) when is_integer(now_ms) do
    clear_stale_pending_state()
    CommandTime.with_now_ms(now_ms, fun)
  end

  defp with_apply_time(_meta, fun) do
    clear_stale_pending_state()
    fun.()
  end

  defp clear_stale_pending_state do
    Process.delete(@sm_apply_state_key)
    :ok
  end

  defp apply_now_ms do
    CommandTime.now_ms()
  end

  defp with_current_ra_index(%{index: ra_index}, fun) when is_integer(ra_index) do
    previous = Process.get(:sm_current_ra_index, :undefined)
    Process.put(:sm_current_ra_index, ra_index)

    try do
      fun.()
    after
      case previous do
        :undefined -> Process.delete(:sm_current_ra_index)
        value -> Process.put(:sm_current_ra_index, value)
      end
    end
  end

  defp with_current_ra_index(_meta, fun), do: fun.()

  defp current_ra_index do
    case Process.get(:sm_current_ra_index) do
      idx when is_integer(idx) and idx >= 0 -> idx
      _ -> nil
    end
  end

  defp raft_apply_hook(%{instance_ctx: %{raft_apply_hook: fun}}) when is_function(fun), do: fun

  defp raft_apply_hook(%{instance_name: name}) when is_atom(name) do
    raft_apply_hook_for_instance(name)
  end

  defp raft_apply_hook(_state), do: raft_apply_hook_for_instance(:default)

  defp raft_apply_hook_for_instance(name) do
    try do
      case FerricStore.Instance.get(name) do
        %{raft_apply_hook: fun} when is_function(fun) -> fun
        _ -> nil
      end
    rescue
      _ -> nil
    catch
      :exit, _ -> nil
    end
  end

  defp apply_pending_with_time(meta, state, fun) do
    with_apply_time(meta, fn ->
      with_current_ra_index(meta, fn ->
        result = with_pending_writes(state, fun)
        bump_applied(meta, state, result)
      end)
    end)
  end

  defp apply_control_with_time(meta, state, fun) do
    with_apply_time(meta, fn ->
      {new_state, result} = fun.()
      old_count = state.applied_count
      new_state = %{new_state | applied_count: old_count + 1}
      maybe_release_cursor(meta, old_count, new_state, result)
    end)
  end

  defp apply_prob_with_time(meta, state, fun) do
    with_apply_time(meta, fn ->
      result = do_prob_command(state, fun)
      bump_applied(meta, state, result)
    end)
  end

  defp cross_shard_ordered_entries(shard_batches) do
    {_next_legacy_index, entries} =
      Enum.reduce(shard_batches, {0, []}, fn {shard_idx, queue, sandbox_namespace},
                                             {next_legacy_index, acc} ->
        {next_legacy_index, batch_entries} =
          queue
          |> Enum.with_index()
          |> Enum.reduce({next_legacy_index, []}, fn
            {{orig_idx, entry}, pos}, {next, inner} when is_integer(orig_idx) ->
              {max(next, orig_idx + 1),
               [{orig_idx, shard_idx, pos, entry, sandbox_namespace} | inner]}

            {entry, pos}, {next, inner} ->
              {next + 1, [{next, shard_idx, pos, entry, sandbox_namespace} | inner]}
          end)

        {next_legacy_index, batch_entries ++ acc}
      end)

    Enum.sort_by(entries, fn {orig_idx, _shard_idx, _pos, _entry, _sandbox_namespace} ->
      orig_idx
    end)
  end

  defp dispatch_cross_shard_entry(
         {:flow_cross_spawn_children, attrs},
         _sandbox_namespace,
         _store,
         state
       ) do
    do_flow_cross_spawn_children(state, attrs)
  end

  defp dispatch_cross_shard_entry(
         {orig_idx, entry},
         sandbox_namespace,
         store,
         state
       )
       when is_integer(orig_idx) and is_tuple(entry) do
    dispatch_cross_shard_entry(entry, sandbox_namespace, store, state)
  end

  defp dispatch_cross_shard_entry(
         {:flow_cross_terminal, op, attrs},
         _sandbox_namespace,
         _store,
         state
       ) do
    if op in [:complete, :retry, :fail, :cancel] do
      do_flow_cross_terminal(state, op, attrs)
    else
      {:error, "ERR invalid flow cross-shard terminal op"}
    end
  end

  defp dispatch_cross_shard_entry(
         {:flow_cross_terminal_many, op, attrs_list},
         _sandbox_namespace,
         _store,
         state
       ) do
    if op in [:complete, :retry, :fail, :cancel] do
      do_flow_cross_terminal_many(state, op, attrs_list)
    else
      {:error, "ERR invalid flow cross-shard terminal op"}
    end
  end

  defp dispatch_cross_shard_entry(entry, sandbox_namespace, store, _state) do
    ast =
      entry
      |> TxAst.command_ast()
      |> TxAst.namespace_first_key(sandbox_namespace)

    try do
      Dispatcher.dispatch_ast(ast, store)
    catch
      :exit, {:noproc, _} ->
        {:error, "ERR server not ready, shard process unavailable"}

      :exit, {reason, _} ->
        {:error, "ERR internal error: #{inspect(reason)}"}
    end
  end

  defp cross_shard_fatal_entry_error({orig_idx, entry}, result)
       when is_integer(orig_idx) and is_tuple(entry) do
    cross_shard_fatal_entry_error(entry, result)
  end

  defp cross_shard_fatal_entry_error(
         _entry,
         {:error, {:blob_externalize_failed, _reason}} = error
       ),
       do: error

  defp cross_shard_fatal_entry_error(
         {:flow_cross_spawn_children, _attrs},
         {:error, _reason} = error
       ),
       do: error

  defp cross_shard_fatal_entry_error(
         {:flow_cross_terminal, _op, _attrs},
         {:error, _reason} = error
       ),
       do: error

  defp cross_shard_fatal_entry_error(
         {:flow_cross_terminal_many, _op, _attrs_list},
         {:error, _reason} = error
       ),
       do: error

  defp cross_shard_fatal_entry_error(_entry, _result), do: nil

  defp cross_shard_results_by_batch_position(shard_batches, results_by_position) do
    Map.new(shard_batches, fn {shard_idx, queue, _sandbox_namespace} ->
      shard_positions = Map.get(results_by_position, shard_idx, %{})

      results =
        queue
        |> Enum.with_index()
        |> Enum.map(fn {_entry, pos} -> Map.fetch!(shard_positions, pos) end)

      {shard_idx, results}
    end)
  end

  # ---------------------------------------------------------------------------
  # Private: cross-shard transaction store builder
  # ---------------------------------------------------------------------------

  # Builds a store map for a given shard_idx, usable by Dispatcher.dispatch.
  # For the anchor shard (matching state.shard_index), uses state directly.
  # For remote shards, reads active file info from persistent_term.
  defp build_cross_shard_store(shard_idx, anchor_state) do
    instance_ctx = cross_shard_instance_ctx(anchor_state)

    data_dir =
      if instance_ctx do
        instance_ctx.data_dir
      else
        anchor_state.data_dir
      end

    default_ctx = cross_shard_ctx(anchor_state, shard_idx, data_dir, instance_ctx)

    ctx_for_key =
      if instance_ctx do
        ctx_by_shard =
          0..(instance_ctx.shard_count - 1)
          |> Map.new(fn idx ->
            {idx, cross_shard_ctx(anchor_state, idx, data_dir, instance_ctx)}
          end)

        fn key ->
          Map.fetch!(ctx_by_shard, cross_shard_route_key(instance_ctx, key, shard_idx))
        end
      else
        fn _key -> default_ctx end
      end

    tx_binary_ref = keydir_binary_ref(anchor_state)

    put_in_ctx = fn ctx, key, value, expire_at_ms ->
      case maybe_externalize_cross_shard_value(anchor_state, ctx, value) do
        {:ok, value_for, disk_val, pending_value} ->
          record_cross_shard_pending_original(ctx, key)

          unless standalone_staged_apply?() do
            if tx_binary_ref do
              new_bytes = binary_byte_size(key) + binary_byte_size(value_for)

              old_bytes =
                case :ets.lookup(ctx.keydir, key) do
                  [{^key, old_val, _, _, _, _, _}] ->
                    binary_byte_size(key) + binary_byte_size(old_val)

                  _ ->
                    0
                end

              delta = new_bytes - old_bytes
              if delta != 0, do: :atomics.add(tx_binary_ref, ctx.index + 1, delta)
            end

            :ets.insert(
              ctx.keydir,
              {key, value_for, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
            )
          end

          sm_tx_put_pending(key, pending_value, expire_at_ms)
          deleted = Process.get(:tx_deleted_keys, MapSet.new())

          if MapSet.member?(deleted, key) do
            Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
          end

          queue_cross_shard_pending_put(ctx, key, disk_val, expire_at_ms, value_for)

          :ok

        {:error, _reason} = error ->
          error
      end
    end

    local_put = fn key, value, expire_at_ms ->
      put_in_ctx.(ctx_for_key.(key), key, value, expire_at_ms)
    end

    delete_in_ctx = fn ctx, key ->
      record_cross_shard_pending_original(ctx, key)

      unless standalone_staged_apply?() do
        if tx_binary_ref do
          bytes =
            case :ets.lookup(ctx.keydir, key) do
              [{^key, val, _, _, _, _, _}] -> binary_byte_size(key) + binary_byte_size(val)
              _ -> 0
            end

          if bytes > 0, do: :atomics.sub(tx_binary_ref, ctx.index + 1, bytes)
        end

        :ets.delete(ctx.keydir, key)
      end

      sm_tx_drop_pending(key)
      deleted = Process.get(:tx_deleted_keys, MapSet.new())
      Process.put(:tx_deleted_keys, MapSet.put(deleted, key))
      queue_cross_shard_pending_delete(ctx, key)
      :ok
    end

    local_delete = fn key ->
      delete_in_ctx.(ctx_for_key.(key), key)
    end

    local_get = fn key ->
      ctx = ctx_for_key.(key)
      deleted = Process.get(:tx_deleted_keys, MapSet.new())

      if MapSet.member?(deleted, key) do
        nil
      else
        case sm_tx_pending_meta(key) do
          {value, _exp} -> normalize_get_value(value)
          nil -> ctx |> cross_shard_ets_read(key) |> normalize_get_value()
        end
      end
    end

    local_get_meta = fn key ->
      ctx = ctx_for_key.(key)
      deleted = Process.get(:tx_deleted_keys, MapSet.new())

      if MapSet.member?(deleted, key) do
        nil
      else
        case sm_tx_pending_meta(key) do
          nil -> cross_shard_ets_read_meta(ctx, key)
          meta -> meta
        end
      end
    end

    local_exists = fn key ->
      ctx = ctx_for_key.(key)
      deleted = Process.get(:tx_deleted_keys, MapSet.new())

      if MapSet.member?(deleted, key) do
        false
      else
        sm_tx_pending_meta(key) != nil or cross_shard_ets_exists?(ctx, key)
      end
    end

    local_incr = fn key, delta ->
      case local_get_meta.(key) do
        nil ->
          local_put.(key, delta, 0)
          {:ok, delta}

        {value, expire_at_ms} ->
          case coerce_integer(value) do
            {:ok, int_val} ->
              new_val = int_val + delta
              local_put.(key, new_val, expire_at_ms)
              {:ok, new_val}

            :error ->
              {:error, "ERR value is not an integer or out of range"}
          end
      end
    end

    local_incr_float = fn key, delta ->
      case local_get_meta.(key) do
        nil ->
          new_val = delta * 1.0
          local_put.(key, new_val, 0)
          {:ok, new_val}

        {value, expire_at_ms} ->
          case coerce_float(value) do
            {:ok, float_val} ->
              new_val = float_val + delta
              local_put.(key, new_val, expire_at_ms)
              {:ok, new_val}

            :error ->
              {:error, "ERR value is not a valid float"}
          end
      end
    end

    local_append = fn key, suffix ->
      {current, expire_at_ms} =
        case local_get_meta.(key) do
          nil -> {"", 0}
          {v, exp} when is_integer(v) -> {Integer.to_string(v), exp}
          {v, exp} when is_float(v) -> {Float.to_string(v), exp}
          {v, exp} -> {v, exp}
        end

      new_val = current <> suffix
      local_put.(key, new_val, expire_at_ms)
      {:ok, byte_size(new_val)}
    end

    local_getset = fn key, new_value ->
      old = local_get.(key)
      local_put.(key, new_value, 0)
      old
    end

    local_getdel = fn key ->
      old = local_get.(key)
      if old, do: local_delete.(key)
      old
    end

    local_getex = fn key, expire_at_ms ->
      value = local_get.(key)
      if value, do: local_put.(key, value, expire_at_ms)
      value
    end

    local_setrange = fn key, offset, value ->
      {old, expire_at_ms} =
        case local_get_meta.(key) do
          nil -> {"", 0}
          {v, exp} when is_integer(v) -> {Integer.to_string(v), exp}
          {v, exp} when is_float(v) -> {Float.to_string(v), exp}
          {v, exp} -> {v, exp}
        end

      new_val = sm_apply_setrange(old, offset, value)
      local_put.(key, new_val, expire_at_ms)
      {:ok, byte_size(new_val)}
    end

    promoted_put = fn redis_key, compound_key, value, expire_at_ms, dedicated_path ->
      ctx = ctx_for_key.(redis_key)
      Promotion.await_compaction_latch(anchor_state, redis_key)

      value_for = value_for_ets(value, hot_cache_threshold(anchor_state))
      disk_val = to_disk_binary(value)
      active = Promotion.find_active(dedicated_path)
      fid = parse_fid_from_path(active)

      case NIF.v2_append_record(active, compound_key, disk_val, expire_at_ms) do
        {:ok, {offset, _record_size}} ->
          value_size = byte_size(disk_val)

          if tx_binary_ref do
            new_bytes = binary_byte_size(compound_key) + binary_byte_size(value_for)

            old_bytes =
              case :ets.lookup(ctx.keydir, compound_key) do
                [{^compound_key, old_val, _, _, _, _, _}] ->
                  binary_byte_size(compound_key) + binary_byte_size(old_val)

                _ ->
                  0
              end

            delta = new_bytes - old_bytes
            if delta != 0, do: :atomics.add(tx_binary_ref, ctx.index + 1, delta)
          end

          :ets.insert(
            ctx.keydir,
            {compound_key, value_for, expire_at_ms, LFU.initial(), fid, offset, value_size}
          )

          sm_tx_put_pending(compound_key, value, expire_at_ms)
          deleted = Process.get(:tx_deleted_keys, MapSet.new())

          if MapSet.member?(deleted, compound_key) do
            Process.put(:tx_deleted_keys, MapSet.delete(deleted, compound_key))
          end

          :ok

        {:error, _reason} = err ->
          err
      end
    end

    promoted_delete = fn redis_key, compound_key, dedicated_path ->
      ctx = ctx_for_key.(redis_key)
      Promotion.await_compaction_latch(anchor_state, redis_key)

      if tx_binary_ref do
        bytes =
          case :ets.lookup(ctx.keydir, compound_key) do
            [{^compound_key, val, _, _, _, _, _}] ->
              binary_byte_size(compound_key) + binary_byte_size(val)

            _ ->
              0
          end

        if bytes > 0, do: :atomics.sub(tx_binary_ref, ctx.index + 1, bytes)
      end

      active = Promotion.find_active(dedicated_path)

      case NIF.v2_append_tombstone(active, compound_key) do
        {:ok, _offset} ->
          :ets.delete(ctx.keydir, compound_key)
          sm_tx_drop_pending(compound_key)
          deleted = Process.get(:tx_deleted_keys, MapSet.new())
          Process.put(:tx_deleted_keys, MapSet.put(deleted, compound_key))
          :ok

        {:error, _reason} = err ->
          err
      end
    end

    promoted_put_batch = fn redis_key, entries, dedicated_path ->
      ctx = ctx_for_key.(redis_key)
      Promotion.await_compaction_latch(anchor_state, redis_key)

      active = Promotion.find_active(dedicated_path)
      fid = parse_fid_from_path(active)

      prepared =
        Enum.map(entries, fn {compound_key, value, expire_at_ms} ->
          value_for = value_for_ets(value, hot_cache_threshold(anchor_state))
          disk_val = to_disk_binary(value)
          {compound_key, value, value_for, disk_val, expire_at_ms}
        end)

      batch =
        Enum.map(prepared, fn {compound_key, _value, _value_for, disk_val, expire_at_ms} ->
          {compound_key, disk_val, expire_at_ms}
        end)

      case NIF.v2_append_batch(active, batch) do
        {:ok, locations} when length(locations) == length(prepared) ->
          deleted =
            Enum.zip(prepared, locations)
            |> Enum.reduce(Process.get(:tx_deleted_keys, MapSet.new()), fn
              {{compound_key, value, value_for, disk_val, expire_at_ms}, {offset, _value_size}},
              deleted_acc ->
                if tx_binary_ref do
                  new_bytes = binary_byte_size(compound_key) + binary_byte_size(value_for)

                  old_bytes =
                    case :ets.lookup(ctx.keydir, compound_key) do
                      [{^compound_key, old_val, _, _, _, _, _}] ->
                        binary_byte_size(compound_key) + binary_byte_size(old_val)

                      _ ->
                        0
                    end

                  delta = new_bytes - old_bytes
                  if delta != 0, do: :atomics.add(tx_binary_ref, ctx.index + 1, delta)
                end

                :ets.insert(
                  ctx.keydir,
                  {
                    compound_key,
                    value_for,
                    expire_at_ms,
                    LFU.initial(),
                    fid,
                    offset,
                    byte_size(disk_val)
                  }
                )

                sm_tx_put_pending(compound_key, value, expire_at_ms)
                MapSet.delete(deleted_acc, compound_key)
            end)

          Process.put(:tx_deleted_keys, deleted)
          :ok

        {:ok, locations} ->
          {:error, {:batch_result_mismatch, length(prepared), locations}}

        {:error, _reason} = err ->
          err
      end
    end

    promoted_delete_batch = fn redis_key, compound_keys, dedicated_path ->
      ctx = ctx_for_key.(redis_key)
      Promotion.await_compaction_latch(anchor_state, redis_key)

      active = Promotion.find_active(dedicated_path)
      ops = Enum.map(compound_keys, &{:delete, &1})

      case NIF.v2_append_ops_batch_nosync(active, ops) do
        {:ok, locations} ->
          with :ok <- validate_promoted_tombstone_batch(locations, length(compound_keys)),
               :ok <- NIF.v2_fsync(active) do
            deleted =
              Enum.reduce(compound_keys, Process.get(:tx_deleted_keys, MapSet.new()), fn
                compound_key, acc ->
                  if tx_binary_ref do
                    bytes =
                      case :ets.lookup(ctx.keydir, compound_key) do
                        [{^compound_key, val, _, _, _, _, _}] ->
                          binary_byte_size(compound_key) + binary_byte_size(val)

                        _ ->
                          0
                      end

                    if bytes > 0, do: :atomics.sub(tx_binary_ref, ctx.index + 1, bytes)
                  end

                  :ets.delete(ctx.keydir, compound_key)
                  sm_tx_drop_pending(compound_key)
                  MapSet.put(acc, compound_key)
              end)

            Process.put(:tx_deleted_keys, deleted)
            :ok
          end

        {:error, _reason} = err ->
          err
      end
    end

    %{
      get: local_get,
      get_meta: local_get_meta,
      batch_get: fn keys -> cross_shard_routed_batch_read(keys, ctx_for_key) end,
      put: local_put,
      delete: local_delete,
      exists?: local_exists,
      keys: fn -> Router.keys(instance_ctx) end,
      flush: fn ->
        Enum.each(Router.keys(instance_ctx), fn k -> Router.delete(instance_ctx, k) end)
        :ok
      end,
      dbsize: fn -> Router.dbsize(instance_ctx) end,
      incr: local_incr,
      incr_float: local_incr_float,
      append: local_append,
      getset: local_getset,
      getdel: local_getdel,
      getex: local_getex,
      setrange: local_setrange,
      cas: fn key, expected, new_value, ttl_ms ->
        Router.cas(instance_ctx, key, expected, new_value, ttl_ms)
      end,
      lock: fn key, owner, ttl_ms -> Router.lock(instance_ctx, key, owner, ttl_ms) end,
      unlock: fn key, owner -> Router.unlock(instance_ctx, key, owner) end,
      extend: fn key, owner, ttl_ms -> Router.extend(instance_ctx, key, owner, ttl_ms) end,
      ratelimit_add: fn key, window_ms, max, count ->
        Router.ratelimit_add(instance_ctx, key, window_ms, max, count)
      end,
      list_op: fn key, op -> Router.list_op(instance_ctx, key, op) end,
      compound_get: fn redis_key, compound_key ->
        ctx = ctx_for_key.(redis_key)
        cross_shard_compound_read(ctx, redis_key, compound_key)
      end,
      compound_get_meta: fn redis_key, compound_key ->
        ctx = ctx_for_key.(redis_key)
        cross_shard_compound_read_meta(ctx, redis_key, compound_key)
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        ctx = ctx_for_key.(redis_key)
        cross_shard_compound_batch_read(ctx, redis_key, compound_keys)
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        ctx = ctx_for_key.(redis_key)
        cross_shard_compound_batch_read_meta(ctx, redis_key, compound_keys)
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        ctx = ctx_for_key.(redis_key)

        case promoted_compound_path(ctx, redis_key, compound_key) do
          nil ->
            put_in_ctx.(ctx, compound_key, value, expire_at_ms)

          dedicated_path ->
            promoted_put.(redis_key, compound_key, value, expire_at_ms, dedicated_path)
        end
      end,
      compound_batch_put: fn redis_key, entries ->
        ctx = ctx_for_key.(redis_key)

        entries
        |> Enum.chunk_by(fn {compound_key, _value, _expire_at_ms} ->
          promoted_compound_path(ctx, redis_key, compound_key)
        end)
        |> Enum.reduce_while(:ok, fn entries, :ok ->
          result =
            case promoted_compound_path(ctx, redis_key, elem(hd(entries), 0)) do
              nil ->
                Enum.each(entries, fn {compound_key, value, expire_at_ms} ->
                  put_in_ctx.(ctx, compound_key, value, expire_at_ms)
                end)

                :ok

              dedicated_path ->
                promoted_put_batch.(redis_key, entries, dedicated_path)
            end

          case result do
            :ok -> {:cont, :ok}
            {:error, _} = error -> {:halt, error}
          end
        end)
      end,
      compound_delete: fn redis_key, compound_key ->
        ctx = ctx_for_key.(redis_key)

        case promoted_compound_path(ctx, redis_key, compound_key) do
          nil -> delete_in_ctx.(ctx, compound_key)
          dedicated_path -> promoted_delete.(redis_key, compound_key, dedicated_path)
        end
      end,
      compound_batch_delete: fn redis_key, compound_keys ->
        ctx = ctx_for_key.(redis_key)

        compound_keys
        |> Enum.chunk_by(fn compound_key ->
          promoted_compound_path(ctx, redis_key, compound_key)
        end)
        |> Enum.reduce_while(:ok, fn compound_keys, :ok ->
          result =
            case promoted_compound_path(ctx, redis_key, hd(compound_keys)) do
              nil ->
                Enum.each(compound_keys, fn compound_key ->
                  delete_in_ctx.(ctx, compound_key)
                end)

                :ok

              dedicated_path ->
                promoted_delete_batch.(redis_key, compound_keys, dedicated_path)
            end

          case result do
            :ok -> {:cont, :ok}
            {:error, _} = error -> {:halt, error}
          end
        end)
      end,
      compound_scan: fn redis_key, prefix ->
        ctx = ctx_for_key.(redis_key)
        cross_shard_compound_scan(ctx, redis_key, prefix)
      end,
      compound_count: fn redis_key, prefix ->
        ctx = ctx_for_key.(redis_key)
        cross_shard_prefix_count(ctx, prefix)
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        ctx = ctx_for_key.(redis_key)
        cross_shard_delete_prefix(ctx, prefix, fn key -> delete_in_ctx.(ctx, key) end)
      end,
      zset_score_range: fn redis_key, min_bound, max_bound, reverse? ->
        ctx = ctx_for_key.(redis_key)

        cross_shard_zset_index_read(ctx, redis_key, fn state ->
          {:ok,
           ZSetIndex.range(state.zset_score_index, redis_key, min_bound, max_bound, reverse?)}
        end)
      end,
      zset_score_range_slice: fn redis_key, min_bound, max_bound, reverse?, offset, count ->
        ctx = ctx_for_key.(redis_key)

        cross_shard_zset_index_read(ctx, redis_key, fn state ->
          {:ok,
           ZSetIndex.range_slice(
             state.zset_score_index,
             redis_key,
             min_bound,
             max_bound,
             reverse?,
             offset,
             count
           )}
        end)
      end,
      zset_score_count: fn redis_key, min_bound, max_bound ->
        ctx = ctx_for_key.(redis_key)

        cross_shard_zset_index_read(ctx, redis_key, fn state ->
          {:ok,
           ZSetIndex.count(
             state.zset_score_index,
             state.zset_score_lookup,
             redis_key,
             min_bound,
             max_bound
           )}
        end)
      end,
      zset_rank_range: fn redis_key, start_idx, stop_idx, reverse? ->
        ctx = ctx_for_key.(redis_key)

        cross_shard_zset_index_read(ctx, redis_key, fn state ->
          {:ok,
           ZSetIndex.rank_range(state.zset_score_index, redis_key, start_idx, stop_idx, reverse?)}
        end)
      end,
      zset_member_rank: fn redis_key, member, reverse? ->
        ctx = ctx_for_key.(redis_key)

        cross_shard_zset_index_read(ctx, redis_key, fn state ->
          {:ok,
           ZSetIndex.member_rank(
             state.zset_score_index,
             state.zset_score_lookup,
             redis_key,
             member,
             reverse?
           )}
        end)
      end,
      prob_dir: fn ->
        Path.join(default_ctx.shard_data_path, "prob")
      end,
      prob_write: fn command ->
        # Within cross-shard tx, prob writes are applied directly
        # (the state machine is already applying through Raft)
        apply_prob_locally(instance_ctx, command)
      end,
      shard_index: default_ctx.index,
      data_dir: data_dir
    }
  end

  defp cross_shard_ctx(anchor_state, shard_idx, data_dir, instance_ctx) do
    if shard_idx == anchor_state.shard_index do
      %{
        instance_ctx: instance_ctx,
        keydir: anchor_state.ets,
        index: shard_idx,
        data_dir: data_dir,
        shard_data_path: anchor_state.shard_data_path,
        active_file_path: anchor_state.active_file_path,
        active_file_id: anchor_state.active_file_id,
        zset_score_index_name: anchor_state.zset_score_index_name,
        zset_score_lookup_name: anchor_state.zset_score_lookup_name,
        flow_index_name: Map.get(anchor_state, :flow_index_name),
        flow_lookup_name: Map.get(anchor_state, :flow_lookup_name)
      }
    else
      {file_id, file_path, shard_data_path} =
        Ferricstore.Store.ActiveFile.get(instance_ctx, shard_idx)

      keydir =
        if instance_ctx do
          elem(instance_ctx.keydir_refs, shard_idx)
        else
          :"keydir_#{shard_idx}"
        end

      instance_name = if instance_ctx, do: instance_ctx.name, else: anchor_state.instance_name

      {zset_score_index_name, zset_score_lookup_name} =
        ZSetIndex.table_names(instance_name, shard_idx)

      {flow_index_name, flow_lookup_name} = FlowIndex.table_names(instance_name, shard_idx)

      %{
        instance_ctx: instance_ctx,
        keydir: keydir,
        index: shard_idx,
        data_dir: data_dir,
        shard_data_path: shard_data_path,
        active_file_path: file_path,
        active_file_id: file_id,
        zset_score_index_name: zset_score_index_name,
        zset_score_lookup_name: zset_score_lookup_name,
        flow_index_name: flow_index_name,
        flow_lookup_name: flow_lookup_name
      }
    end
  end

  defp cross_shard_route_key(%{slot_map: _} = instance_ctx, key, _default_idx) do
    Router.shard_for(instance_ctx, key)
  end

  defp cross_shard_route_key(_instance_ctx, _key, default_idx), do: default_idx

  defp cross_shard_state_for_key(anchor_state, key) when is_binary(key) do
    instance_ctx = cross_shard_instance_ctx(anchor_state)

    shard_idx =
      if instance_ctx, do: Router.shard_for(instance_ctx, key), else: anchor_state.shard_index

    ctx = cross_shard_ctx(anchor_state, shard_idx, anchor_state.data_dir, instance_ctx)

    %{
      anchor_state
      | shard_index: ctx.index,
        ets: ctx.keydir,
        shard_data_path: ctx.shard_data_path,
        shard_data_path_expanded: Path.expand(ctx.shard_data_path),
        active_file_path: ctx.active_file_path,
        active_file_id: ctx.active_file_id,
        zset_score_index_name: ctx.zset_score_index_name,
        zset_score_lookup_name: ctx.zset_score_lookup_name,
        flow_index_name: ctx.flow_index_name,
        flow_lookup_name: ctx.flow_lookup_name,
        flow_lmdb_path: Ferricstore.Flow.LMDB.path(ctx.shard_data_path),
        flow_lmdb_mirror?: Ferricstore.Flow.LMDB.mirror?()
    }
  end

  defp cross_shard_instance_ctx(%{instance_ctx: %FerricStore.Instance{} = ctx} = state) do
    if instance_data_path?(ctx, state), do: ctx, else: nil
  end

  defp cross_shard_instance_ctx(%{instance_ctx: ctx}) when is_map(ctx) do
    if Map.has_key?(ctx, :shard_count) and Map.has_key?(ctx, :keydir_refs) do
      ctx
    else
      nil
    end
  end

  defp cross_shard_instance_ctx(state) do
    case instance_ctx_for_state(state) do
      %FerricStore.Instance{} = ctx ->
        if instance_data_path?(ctx, state), do: ctx, else: nil

      ctx when is_map(ctx) ->
        if Map.has_key?(ctx, :shard_count) and Map.has_key?(ctx, :keydir_refs) do
          ctx
        else
          nil
        end

      _ ->
        nil
    end
  end

  defp sm_tx_pending_meta(key) do
    pending = Process.get(:tx_pending_values, %{})

    case Map.get(pending, key) do
      {value, 0} ->
        {value, 0}

      {value, exp} ->
        if exp > apply_now_ms() do
          {value, exp}
        else
          sm_tx_drop_pending(key)
          nil
        end

      nil ->
        nil
    end
  end

  defp normalize_get_value(nil), do: nil
  defp normalize_get_value(value) when is_integer(value), do: Integer.to_string(value)

  defp normalize_get_value(value) when is_float(value),
    do: Ferricstore.Store.ValueCodec.format_float(value)

  defp normalize_get_value(value), do: value

  defp sm_tx_put_pending(key, value, expire_at_ms) do
    pending = Process.get(:tx_pending_values, %{})
    Process.put(:tx_pending_values, Map.put(pending, key, {value, expire_at_ms}))

    deleted = Process.get(:tx_deleted_keys, MapSet.new())

    if MapSet.member?(deleted, key) do
      Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
    end
  end

  defp sm_tx_drop_pending(key) do
    pending = Process.get(:tx_pending_values, %{})
    Process.put(:tx_pending_values, Map.delete(pending, key))
  end

  defp sm_merge_tx_pending_prefix(results, prefix) do
    deleted = Process.get(:tx_deleted_keys, MapSet.new())
    prefix_len = byte_size(prefix)

    base =
      results
      |> Enum.reject(fn {field, _value} -> MapSet.member?(deleted, prefix <> field) end)
      |> Map.new()

    Process.get(:tx_pending_values, %{})
    |> Enum.reduce(base, fn
      {key, {value, exp}}, acc when is_binary(key) and byte_size(key) >= prefix_len ->
        if String.starts_with?(key, prefix) and not MapSet.member?(deleted, key) and
             (exp == 0 or exp > apply_now_ms()) do
          field =
            case :binary.split(key, <<0>>) do
              [_pre, sub] -> sub
              _ -> key
            end

          Map.put(acc, field, value)
        else
          acc
        end

      _other, acc ->
        acc
    end)
    |> Map.to_list()
    |> Enum.sort_by(fn {field, _value} -> field end)
  end

  # Reads a value from a shard's keydir ETS table with cold-read fallback.
  defp cross_shard_ets_read(ctx, key) do
    cross_shard_ets_read_from_path(ctx, key, ctx.shard_data_path)
  end

  defp cross_shard_ets_exists?(ctx, key) do
    now = apply_now_ms()

    try do
      case :ets.lookup(ctx.keydir, key) do
        [{^key, value, exp, _lfu, _fid, _off, _vsize}]
        when value != nil and (exp == 0 or exp > now) ->
          true

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when (exp == 0 or exp > now) and valid_cold_location(fid, off, vsize) ->
          true

        [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
          cross_shard_delete_keydir_entry(ctx, key, value)
          false

        _ ->
          false
      end
    rescue
      ArgumentError ->
        emit_cross_shard_keydir_unavailable(ctx, :cross_shard_exists)
        false
    end
  end

  defp cross_shard_ets_read_from_path(ctx, key, data_path) do
    now = apply_now_ms()

    try do
      case :ets.lookup(ctx.keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          value

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          path = sm_file_path_from_path(data_path, fid)

          case read_cold_async(path, off, key) do
            {:ok, v} -> materialize_cross_shard_cold_value(ctx, v)
            _ -> nil
          end

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          value

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          path = sm_file_path_from_path(data_path, fid)

          case read_cold_async(path, off, key) do
            {:ok, v} -> materialize_cross_shard_cold_value(ctx, v)
            _ -> nil
          end

        [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
          cross_shard_delete_keydir_entry(ctx, key, nil)
          nil

        _ ->
          nil
      end
    rescue
      ArgumentError ->
        emit_cross_shard_keydir_unavailable(ctx, :cross_shard_get)
        nil
    end
  end

  # Reads value + expire_at_ms from a shard's keydir ETS table.
  defp cross_shard_ets_read_meta(ctx, key) do
    cross_shard_ets_read_meta_from_path(ctx, key, ctx.shard_data_path)
  end

  defp materialize_cross_shard_cold_value(ctx, value) do
    threshold = BlobValue.threshold(Map.get(ctx, :instance_ctx))

    case BlobValue.maybe_materialize(ctx.data_dir, ctx.index, threshold, value) do
      {:ok, materialized} -> materialized
      {:error, _reason} -> nil
    end
  end

  defp cross_shard_ets_read_meta_from_path(ctx, key, data_path) do
    now = apply_now_ms()

    try do
      case :ets.lookup(ctx.keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          {value, 0}

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          path = sm_file_path_from_path(data_path, fid)

          case read_cold_async(path, off, key) do
            {:ok, v} -> {materialize_cross_shard_cold_value(ctx, v), 0}
            _ -> nil
          end

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {value, exp}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          path = sm_file_path_from_path(data_path, fid)

          case read_cold_async(path, off, key) do
            {:ok, v} -> {materialize_cross_shard_cold_value(ctx, v), exp}
            _ -> nil
          end

        [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
          cross_shard_delete_keydir_entry(ctx, key, nil)
          nil

        _ ->
          nil
      end
    rescue
      ArgumentError ->
        emit_cross_shard_keydir_unavailable(ctx, :cross_shard_get_meta)
        nil
    end
  end

  defp cross_shard_prefix_scan(ctx, prefix) do
    cross_shard_prefix_scan_from_path(ctx, prefix, ctx.shard_data_path)
  end

  defp cross_shard_prefix_scan_from_path(ctx, prefix, data_path) do
    now = apply_now_ms()
    prefix_len = byte_size(prefix)

    ms = [
      {{:"$1", :"$2", :"$3", :_, :"$4", :"$5", :"$6"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [{{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6"}}]}
    ]

    try do
      {tokens, cold_entries, _cold_count} =
        :ets.select(ctx.keydir, ms)
        |> Enum.reduce({[], [], 0}, fn {key, value, exp, fid, off, vsize},
                                       {tokens, cold_entries, cold_count} ->
          cond do
            exp != 0 and exp <= now ->
              {tokens, cold_entries, cold_count}

            value == nil and not valid_cold_location_value?(fid, off, vsize) ->
              cross_shard_delete_keydir_entry(ctx, key, nil)
              {tokens, cold_entries, cold_count}

            value == nil ->
              field = sm_prefix_field(key)
              path = sm_file_path_from_path(data_path, fid)
              entry = {field, key, path, off}
              {[{:cold, cold_count} | tokens], [entry | cold_entries], cold_count + 1}

            true ->
              {[{:value, {sm_prefix_field(key), value}} | tokens], cold_entries, cold_count}
          end
        end)

      cold_values =
        cold_entries
        |> Enum.reverse()
        |> cross_shard_read_cold_batch()
        |> List.to_tuple()

      tokens
      |> Enum.flat_map(fn
        {:value, result} ->
          [result]

        {:cold, index} ->
          case elem(cold_values, index) do
            nil -> []
            result -> [result]
          end
      end)
      |> Enum.sort_by(fn {field, _} -> field end)
    rescue
      ArgumentError ->
        emit_cross_shard_keydir_unavailable(ctx, :cross_shard_prefix_scan)
        []
    end
  end

  defp cross_shard_read_cold_batch([]), do: []

  defp cross_shard_read_cold_batch(entries) do
    locations = Enum.map(entries, fn {_field, key, path, off} -> {path, off, key} end)

    values =
      locations
      |> Ferricstore.Store.ColdRead.pread_batch_keyed(@cold_read_timeout_ms)
      |> normalize_state_machine_batch_values(length(entries))

    emit_state_machine_batch_cold_errors(entries, values, fn {_field, _key, path, _off} ->
      path
    end)

    Enum.zip(entries, values)
    |> Enum.map(fn
      {{field, _key, _path, _off}, value} when is_binary(value) -> {field, value}
      {_entry, _value} -> nil
    end)
  end

  defp normalize_state_machine_batch_values({:ok, values}, count)
       when is_list(values) and length(values) == count,
       do: values

  defp normalize_state_machine_batch_values({:ok, _bad_values}, count),
    do: List.duplicate({:error, :batch_result_length_mismatch}, count)

  defp normalize_state_machine_batch_values({:error, reason}, count),
    do: List.duplicate({:error, reason}, count)

  defp emit_state_machine_batch_cold_errors(entries, values, path_fun) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce(%{}, fn
      {entry, {:error, raw_reason}}, acc ->
        path = path_fun.(entry)
        Map.update(acc, {path, raw_reason}, 1, &(&1 + 1))

      {_entry, _value}, acc ->
        acc
    end)
    |> Enum.each(fn {{path, raw_reason}, count} ->
      ColdRead.emit_pread_error(path, raw_reason, count)
    end)
  end

  defp sm_prefix_field(key) do
    case :binary.split(key, <<0>>) do
      [_pre, sub] -> sub
      _ -> key
    end
  end

  defp cross_shard_compound_read(ctx, redis_key, compound_key) do
    case sm_tx_pending_meta(compound_key) do
      {value, _exp} ->
        value

      nil ->
        case promoted_compound_path(ctx, redis_key, compound_key) do
          nil -> cross_shard_ets_read(ctx, compound_key)
          dedicated_path -> cross_shard_ets_read_from_path(ctx, compound_key, dedicated_path)
        end
    end
  end

  defp cross_shard_compound_read_meta(ctx, redis_key, compound_key) do
    case sm_tx_pending_meta(compound_key) do
      nil ->
        case promoted_compound_path(ctx, redis_key, compound_key) do
          nil -> cross_shard_ets_read_meta(ctx, compound_key)
          dedicated_path -> cross_shard_ets_read_meta_from_path(ctx, compound_key, dedicated_path)
        end

      meta ->
        meta
    end
  end

  defp cross_shard_compound_batch_read(ctx, redis_key, compound_keys) do
    ctx
    |> cross_shard_compound_batch_read_meta(redis_key, compound_keys)
    |> Enum.map(fn
      {value, _exp} -> value
      nil -> nil
    end)
  end

  defp cross_shard_batch_read(ctx, keys) do
    deleted = Process.get(:tx_deleted_keys, MapSet.new())

    entries =
      keys
      |> Enum.with_index()
      |> Enum.reject(fn {key, _index} -> MapSet.member?(deleted, key) end)

    entries
    |> Enum.map(fn {key, _index} -> key end)
    |> then(&cross_shard_ets_batch_read_meta_from_path(ctx, &1, ctx.shard_data_path))
    |> Enum.map(fn
      {value, _exp} -> value
      nil -> nil
    end)
    |> then(&merge_indexed_values(%{}, entries, &1))
    |> values_for_indexes(keys)
  end

  defp cross_shard_routed_batch_read(keys, ctx_for_key) do
    grouped =
      keys
      |> Enum.with_index()
      |> Enum.reduce(%{}, fn {key, index}, acc ->
        ctx = ctx_for_key.(key)

        Map.update(acc, ctx.index, {ctx, [{key, index}]}, fn {existing_ctx, entries} ->
          {existing_ctx, [{key, index} | entries]}
        end)
      end)

    results =
      Enum.reduce(grouped, %{}, fn {_idx, {ctx, entries}}, acc ->
        ordered = Enum.reverse(entries)
        shard_keys = Enum.map(ordered, fn {key, _index} -> key end)
        shard_values = cross_shard_batch_read(ctx, shard_keys)

        ordered
        |> Enum.zip(shard_values)
        |> Enum.reduce(acc, fn {{_key, index}, value}, inner ->
          Map.put(inner, index, value)
        end)
      end)

    keys
    |> Enum.with_index()
    |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
  end

  defp cross_shard_compound_batch_read_meta(_ctx, _redis_key, []), do: []

  defp cross_shard_compound_batch_read_meta(ctx, redis_key, compound_keys) do
    data_path =
      case promoted_compound_path(ctx, redis_key, hd(compound_keys)) do
        nil -> ctx.shard_data_path
        dedicated_path -> dedicated_path
      end

    cross_shard_ets_batch_read_meta_from_path(ctx, compound_keys, data_path)
  end

  defp cross_shard_ets_batch_read_meta_from_path(ctx, keys, data_path) do
    now = apply_now_ms()

    {warm_results, cold_reads} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {key, index}, {results, cold} ->
        case sm_tx_pending_meta(key) do
          {value, exp} ->
            {Map.put(results, index, {value, exp}), cold}

          nil ->
            cross_shard_collect_batch_ets(ctx, key, index, data_path, now, results, cold)
        end
      end)

    results = cross_shard_read_cold_meta_batch(warm_results, Enum.reverse(cold_reads))

    keys
    |> Enum.with_index()
    |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
  end

  defp cross_shard_collect_batch_ets(ctx, key, index, data_path, now, results, cold) do
    case :ets.lookup(ctx.keydir, key) do
      [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
        {Map.put(results, index, {value, 0}), cold}

      [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        path = sm_file_path_from_path(data_path, fid)
        {results, [{index, key, path, off, 0} | cold]}

      [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        {Map.put(results, index, {value, exp}), cold}

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when exp > now and valid_cold_location(fid, off, vsize) ->
        path = sm_file_path_from_path(data_path, fid)
        {results, [{index, key, path, off, exp} | cold]}

      [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
        cross_shard_delete_keydir_entry(ctx, key, nil)
        {results, cold}

      _ ->
        {results, cold}
    end
  rescue
    ArgumentError ->
      {results, cold}
  end

  defp cross_shard_read_cold_meta_batch(results, []), do: results

  defp cross_shard_read_cold_meta_batch(results, cold_reads) do
    locations = Enum.map(cold_reads, fn {_index, key, path, off, _exp} -> {path, off, key} end)

    values =
      locations
      |> Ferricstore.Store.ColdRead.pread_batch_keyed(@cold_read_timeout_ms)
      |> normalize_state_machine_batch_values(length(cold_reads))

    emit_state_machine_batch_cold_errors(cold_reads, values, fn {_index, _key, path, _off, _exp} ->
      path
    end)

    cold_reads
    |> Enum.zip(values)
    |> Enum.reduce(results, fn
      {{index, _key, _path, _off, exp}, value}, acc when is_binary(value) ->
        Map.put(acc, index, {value, exp})

      {_read, _value}, acc ->
        acc
    end)
  end

  defp cross_shard_compound_scan(ctx, redis_key, prefix) do
    results =
      case promoted_compound_path(ctx, redis_key, prefix) do
        nil -> cross_shard_prefix_scan(ctx, prefix)
        dedicated_path -> cross_shard_prefix_scan_from_path(ctx, prefix, dedicated_path)
      end

    sm_merge_tx_pending_prefix(results, prefix)
  end

  defp cross_shard_zset_index_read(ctx, redis_key, fun) do
    cond do
      not sm_zset_index_clean?() ->
        :unavailable

      not zset_index_tables?(ctx) ->
        :unavailable

      true ->
        ctx
        |> cross_shard_zset_index_state(redis_key)
        |> fun.()
    end
  end

  defp cross_shard_zset_index_state(ctx, redis_key) do
    prefix = CompoundKey.zset_prefix(redis_key)
    data_path = promoted_compound_path(ctx, redis_key, prefix) || ctx.shard_data_path

    state = %{
      keydir: ctx.keydir,
      shard_data_path: ctx.shard_data_path,
      zset_score_index: ctx.zset_score_index_name,
      zset_score_lookup: ctx.zset_score_lookup_name,
      zset_index_ready: MapSet.new()
    }

    ZSetIndex.ensure(state, redis_key, prefix, data_path)
  end

  defp zset_index_tables?(%{zset_score_index_name: index, zset_score_lookup_name: lookup})
       when is_atom(index) and is_atom(lookup) do
    :ets.info(index) != :undefined and :ets.info(lookup) != :undefined
  end

  defp zset_index_tables?(_ctx), do: false

  defp sm_zset_index_clean? do
    map_size(Process.get(:tx_pending_values, %{})) == 0 and
      MapSet.size(Process.get(:tx_deleted_keys, MapSet.new())) == 0
  end

  defp promoted_compound_path(ctx, redis_key, compound_key_or_prefix) do
    case compound_type_from_key(compound_key_or_prefix) do
      nil ->
        nil

      type ->
        path = Promotion.dedicated_path(promoted_data_dir(ctx), ctx_index(ctx), type, redis_key)
        if Ferricstore.FS.dir?(path), do: path, else: nil
    end
  end

  defp ctx_index(%{index: index}), do: index
  defp ctx_index(%{shard_index: index}), do: index

  defp promoted_data_dir(%{data_dir: data_dir}) do
    cond do
      Ferricstore.FS.dir?(Path.join(data_dir, "dedicated")) ->
        data_dir

      Ferricstore.FS.dir?(Path.join(Path.dirname(data_dir), "dedicated")) ->
        Path.dirname(data_dir)

      true ->
        data_dir
    end
  end

  defp compound_type_from_key("H:" <> _), do: :hash
  defp compound_type_from_key("S:" <> _), do: :set
  defp compound_type_from_key("Z:" <> _), do: :zset
  defp compound_type_from_key(_), do: nil

  defp cross_shard_prefix_count(ctx, prefix) do
    prefix_len = byte_size(prefix)
    now = apply_now_ms()

    ms = [
      {{:"$1", :_, :"$2", :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$2"]}
    ]

    try do
      :ets.select(ctx.keydir, ms)
      |> Enum.count(fn exp -> exp == 0 or exp > now end)
    rescue
      ArgumentError ->
        emit_cross_shard_keydir_unavailable(ctx, :cross_shard_prefix_count)
        0
    end
  end

  defp emit_cross_shard_keydir_unavailable(ctx, request) do
    :telemetry.execute(
      [:ferricstore, :store, :shard_unavailable],
      %{count: 1},
      %{
        request: request,
        reason: :keydir_unavailable,
        shard_index: Map.get(ctx, :index),
        source: :raft_apply
      }
    )
  end

  defp cross_shard_delete_prefix(ctx, prefix, delete_fn) do
    prefix_len = byte_size(prefix)

    ms = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    try do
      keys = :ets.select(ctx.keydir, ms)
      Enum.each(keys, fn key -> delete_fn.(key) end)
    rescue
      ArgumentError ->
        emit_cross_shard_keydir_unavailable(ctx, :cross_shard_delete_prefix)
        :ok
    end

    :ok
  end

  defp sm_file_path_from_path(data_path, file_id) do
    Path.join(
      data_path,
      "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
    )
  end

  defp valid_cold_location_value?(file_id, offset, value_size) do
    is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
      is_integer(value_size) and value_size >= 0
  end

  defp cross_shard_delete_keydir_entry(ctx, key, value) do
    ref = keydir_binary_ref(ctx)

    if ref do
      bytes = binary_byte_size(key) + binary_byte_size(value)
      if bytes > 0, do: :atomics.sub(ref, ctx.index + 1, bytes)
    end

    :ets.delete(ctx.keydir, key)
  end

  defp parse_fid_from_path(path) do
    path
    |> Path.basename()
    |> String.trim_trailing(".log")
    |> String.to_integer()
  end

  # ---------------------------------------------------------------------------
  # Private: async origin-skip detection
  # ---------------------------------------------------------------------------

  # Returns true when the local ETS already contains an entry for the key
  # targeted by the inner async command. This is how each node decides
  # whether it was the origin (Router already wrote) or a replica (empty
  # ETS, needs to apply). Deterministic per-node because it reads the
  # node's own local ETS state.
  defp async_key_present?(state, {:put, key, _value, _exp}), do: ets_has?(state.ets, key)
  # Delete/getdel: Router deletes from ETS before Raft submit, so ets_has?
  # always returns false on origin. Always apply — tombstone writes are idempotent.
  defp async_key_present?(_state, {:delete, _key}), do: false
  defp async_key_present?(state, {:incr, key, _delta}), do: ets_has?(state.ets, key)
  defp async_key_present?(state, {:incr_float, key, _delta}), do: ets_has?(state.ets, key)
  defp async_key_present?(state, {:append, key, _suffix}), do: ets_has?(state.ets, key)
  defp async_key_present?(state, {:getset, key, _v}), do: ets_has?(state.ets, key)
  defp async_key_present?(_state, {:getdel, _key}), do: false
  defp async_key_present?(state, {:getex, key, _exp}), do: ets_has?(state.ets, key)
  defp async_key_present?(state, {:setrange, key, _off, _v}), do: ets_has?(state.ets, key)
  defp async_key_present?(state, {:setbit, key, _off, _bit}), do: ets_has?(state.ets, key)

  defp async_key_present?(state, {:hincrby, key, field, _delta}) do
    ets_has?(state.ets, Ferricstore.Store.CompoundKey.hash_field(key, field))
  end

  defp async_key_present?(state, {:hincrbyfloat, key, field, _delta}) do
    ets_has?(state.ets, Ferricstore.Store.CompoundKey.hash_field(key, field))
  end

  defp async_key_present?(state, {:zincrby, key, _incr, member}) do
    ets_has?(state.ets, Ferricstore.Store.CompoundKey.zset_member(key, member))
  end

  # List ops check the canonical type marker written by the origin before
  # submit. On replicas the marker is absent, so they apply the inner op.
  defp async_key_present?(state, {:list_op, key, _op}) do
    ets_has?(state.ets, Ferricstore.Store.CompoundKey.type_key(key))
  end

  defp async_key_present?(state, {:list_op_lmove, src_key, _dst, _from, _to}) do
    ets_has?(state.ets, Ferricstore.Store.CompoundKey.type_key(src_key))
  end

  # Unknown inner command shape — conservative fallback: apply it (treat as replica).
  defp async_key_present?(_state, _other), do: false

  defp release_cursor_poke_command?({:release_cursor_poke, index})
       when is_integer(index) and index >= 0,
       do: true

  defp release_cursor_poke_command?({:async, _origin, {:release_cursor_poke, index}})
       when is_integer(index) and index >= 0,
       do: true

  defp release_cursor_poke_command?({:async, {:release_cursor_poke, index}})
       when is_integer(index) and index >= 0,
       do: true

  defp release_cursor_poke_command?(_command), do: false

  defp ets_has?(ets, key) do
    case :ets.lookup(ets, key) do
      [] -> false
      _ -> true
    end
  end

  # ---------------------------------------------------------------------------
  # Private: command execution
  # ---------------------------------------------------------------------------

  # 3-tuple async clauses (current shape, with origin node tag).
  #
  # Origin node decides skip vs apply: each peer compares the embedded
  # `origin` against its own `node()`. Deterministic and correct even when
  # the same key receives multiple RMW commands in rapid succession.
  #
  # Single-node mode (no Erlang distribution) reports `node() == :nonode@nohost`,
  # which equals the originating node by the same name — so the origin-skip
  # still fires correctly and avoids the double-write.

  # Async PUT, origin: skip ETS (Router already inserted) but accumulate
  # disk write only for small values (file_id == :pending means Router
  # deferred disk write to us). Large values already have a real file_id
  # and offset from Router's synchronous NIF write — skip disk too.
  defp apply_single(state, {:async, origin, {:put, key, value, expire_at_ms} = _inner})
       when origin == node() do
    apply_origin_async_put(state, key, value, expire_at_ms)
  end

  # Async PUT, replica: apply normally (both ETS + disk).
  defp apply_single(state, {:async, _origin, {:put, key, value, expire_at_ms}}) do
    apply_single(state, {:put, key, value, expire_at_ms})
  end

  # DELETE/GETDEL are idempotent and must persist an accepted tombstone on the
  # origin even when Router already removed the ETS row. Router's local
  # BitcaskWriter tombstone is asynchronous and can fail independently; the Ra
  # entry is the authoritative repair path.
  defp apply_single(state, {:async, origin, {:delete, key}}) when origin == node() do
    apply_single(state, {:delete, key})
  end

  defp apply_single(state, {:async, origin, {:getdel, key}}) when origin == node() do
    if ets_has?(state.ets, key) do
      apply_single(state, {:getdel, key})
    else
      _ = apply_single(state, {:delete, key})
      nil
    end
  end

  defp apply_single(
         state,
         {:async, origin,
          {:origin_checked, key, inner_cmd, before_value, before_expire_at_ms, expected_value,
           expire_at_ms}}
       )
       when origin == node() do
    case origin_replay_decision(
           state,
           key,
           inner_cmd,
           before_value,
           before_expire_at_ms,
           expected_value,
           expire_at_ms
         ) do
      :already_applied ->
        maybe_queue_already_applied_origin_put(
          state,
          key,
          inner_cmd,
          expected_value,
          expire_at_ms
        )

      :apply ->
        apply_single(state, inner_cmd)

      :apply_expected ->
        apply_origin_checked_expected(
          state,
          key,
          inner_cmd,
          before_value,
          expected_value,
          expire_at_ms
        )

      :newer_local_value ->
        :ok
    end
  end

  defp apply_single(
         state,
         {:async, origin, {:origin_checked, key, inner_cmd, expected_value, expire_at_ms}}
       )
       when origin == node() do
    if origin_command_already_applied?(state, key, inner_cmd, expected_value, expire_at_ms) do
      :ok
    else
      apply_single(state, inner_cmd)
    end
  end

  # Other async commands, origin: skip when Router already applied locally.
  # If recovery has no local marker/value, apply the accepted Ra entry so an
  # origin crash after Ra acceptance cannot lose the command.
  defp apply_single(state, {:async, origin, inner_cmd}) when origin == node() do
    if async_key_present?(state, inner_cmd), do: :ok, else: apply_single(state, inner_cmd)
  end

  defp apply_single(
         state,
         {:async, _origin,
          {:origin_checked, _key, inner_cmd, _before_value, _before_exp, _value, _exp}}
       ) do
    apply_single(state, inner_cmd)
  end

  defp apply_single(state, {:async, _origin, {:origin_checked, _key, inner_cmd, _value, _exp}}) do
    apply_single(state, inner_cmd)
  end

  # Other async commands, replica: apply.
  defp apply_single(state, {:async, _origin, inner_cmd}) do
    apply_single(state, inner_cmd)
  end

  # 2-tuple async clauses (legacy shape from binaries before origin tagging).
  # Kept for WAL backward compatibility — replays still work. New writes use
  # the 3-tuple form. Falls back to the ETS-presence heuristic which is
  # imperfect for repeated RMW on the same key but correct for the common
  # case (single put/delete/incr per key per batch).
  defp apply_single(state, {:async, {:put, key, value, expire_at_ms} = _inner}) do
    if async_key_present?(state, {:put, key, value, expire_at_ms}) do
      maybe_queue_origin_pending_put(state, key, value, expire_at_ms)
      :ok
    else
      apply_single(state, {:put, key, value, expire_at_ms})
    end
  end

  defp apply_single(state, {:async, inner_cmd}) do
    if async_key_present?(state, inner_cmd) do
      :ok
    else
      apply_single(state, inner_cmd)
    end
  end

  defp apply_single(_state, {:release_cursor_poke, index})
       when is_integer(index) and index >= 0 do
    :ok
  end

  defp apply_single(state, {:put, key, value, expire_at_ms}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_put(state, key, value, expire_at_ms)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:put_blob_ref, key, encoded_ref, expire_at_ms}) do
    do_checked_put_blob_ref(state, key, encoded_ref, expire_at_ms)
  end

  defp apply_single(state, {:set, key, value, expire_at_ms, opts}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_set(state, key, value, expire_at_ms, opts)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:set_blob_ref, key, encoded_ref, expire_at_ms, opts}) do
    do_checked_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts)
  end

  defp apply_single(state, {:delete, key}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_delete(state, key)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:delete_prefix, prefix}) do
    do_delete_prefix(state, prefix)
  end

  defp apply_single(state, {:list_op, key, operation}) do
    do_checked_list_op(state, key, operation)
  end

  # When a `:list_op_lmove` arrives on a replica wrapped as `{:async, origin, cmd}`,
  # the 3-tuple async clause unwraps and re-dispatches via apply_single. We need
  # to handle the inner shape here so followers re-execute and converge.
  defp apply_single(state, {:list_op_lmove, src_key, dst_key, from_dir, to_dir}) do
    do_checked_lmove(state, src_key, dst_key, from_dir, to_dir)
  end

  defp apply_single(state, {:compound_put, compound_key, value, expire_at_ms}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_compound_put(state, redis_key, compound_key, value, expire_at_ms)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:compound_put_blob_ref, compound_key, encoded_ref, expire_at_ms}) do
    do_checked_compound_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms)
  end

  defp apply_single(state, {:compound_batch_put, redis_key, entries}) do
    case apply_compound_batch_put_entries(state, redis_key, entries) do
      results when is_list(results) -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp apply_single(state, {:compound_blob_batch_put, redis_key, entries}) do
    case apply_compound_blob_batch_put_entries(state, redis_key, entries) do
      results when is_list(results) -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp apply_single(state, {:compound_delete, compound_key}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_compound_delete(state, redis_key, compound_key)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:compound_batch_delete, redis_key, compound_keys}) do
    case apply_compound_batch_delete_keys(state, redis_key, compound_keys) do
      results when is_list(results) -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp apply_single(state, {:compound_delete_prefix, prefix}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_compound_delete_prefix(state, redis_key, prefix)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:pfadd, key, elements}) do
    HyperLogLog.handle_ast({:pfadd, [key | elements]}, build_string_value_store(state))
  end

  defp apply_single(state, {:pfmerge, dest_key, source_sketches}) do
    do_pfmerge(state, dest_key, source_sketches)
  end

  defp apply_single(state, {:pfmerge, dest_key, _source_keys, source_sketches}) do
    do_pfmerge(state, dest_key, source_sketches)
  end

  defp apply_single(state, {:json_set, key, path, value, flags}) do
    Json.handle_ast({:json_set, key, path, value, flags}, build_string_value_store(state))
  end

  defp apply_single(state, {:json_del, key, path}) do
    Json.handle_ast({:json_del, key, path}, build_string_value_store(state))
  end

  defp apply_single(state, {:json_numincrby, key, path, increment}) do
    Json.handle_ast({:json_numincrby, key, path, increment}, build_string_value_store(state))
  end

  defp apply_single(state, {:json_arrappend, key, path, values}) do
    Json.handle_ast({:json_arrappend, key, path, values}, build_string_value_store(state))
  end

  defp apply_single(state, {:json_toggle, key, path}) do
    Json.handle_ast({:json_toggle, key, path}, build_string_value_store(state))
  end

  defp apply_single(state, {:json_clear, key, path}) do
    Json.handle_ast({:json_clear, key, path}, build_string_value_store(state))
  end

  defp apply_single(state, {:incr, key, delta}) do
    do_incr(state, key, delta)
  end

  defp apply_single(state, {:incr_float, key, delta}) do
    do_incr_float(state, key, delta)
  end

  defp apply_single(state, {:append, key, suffix}) do
    do_append(state, key, suffix)
  end

  defp apply_single(state, {:append_blob_ref, key, encoded_ref}) do
    do_append_blob_ref(state, key, encoded_ref)
  end

  defp apply_single(state, {:getset, key, new_value}) do
    do_getset(state, key, new_value)
  end

  defp apply_single(state, {:getset_blob_ref, key, encoded_ref}) do
    do_getset_blob_ref(state, key, encoded_ref)
  end

  defp apply_single(state, {:getdel, key}) do
    do_getdel(state, key)
  end

  defp apply_single(state, {:getex, key, expire_at_ms}) do
    do_getex(state, key, expire_at_ms)
  end

  defp apply_single(state, {:setrange, key, offset, value}) do
    do_setrange(state, key, offset, value)
  end

  defp apply_single(state, {:setrange_blob_ref, key, offset, encoded_ref}) do
    do_setrange_blob_ref(state, key, offset, encoded_ref)
  end

  defp apply_single(state, {:setbit, key, offset, bit_val}) do
    do_setbit(state, key, offset, bit_val)
  end

  defp apply_single(state, {:hincrby, key, field, delta}) do
    do_hincrby(state, key, field, delta)
  end

  defp apply_single(state, {:hincrbyfloat, key, field, delta}) do
    do_hincrbyfloat(state, key, field, delta)
  end

  defp apply_single(state, {:zincrby, key, increment, member}) do
    do_zincrby(state, key, increment, member)
  end

  defp apply_single(state, {:spop, key, count}) do
    do_spop(state, key, count, 0)
  end

  defp apply_single(state, {:zpop, key, count, direction}) do
    do_zpop(state, key, count, direction)
  end

  defp apply_single(state, {:cas, key, expected, new_value, ttl_ms}) do
    do_cas(state, key, expected, new_value, ttl_ms)
  end

  defp apply_single(state, {:cas_blob_ref, key, expected, encoded_ref, ttl_ms}) do
    do_cas_blob_ref(state, key, expected, encoded_ref, ttl_ms)
  end

  defp apply_single(state, {:lock, key, owner, ttl_ms}) do
    do_lock(state, key, owner, ttl_ms)
  end

  defp apply_single(state, {:unlock, key, owner}) do
    do_unlock(state, key, owner)
  end

  defp apply_single(state, {:extend, key, owner, ttl_ms}) do
    do_extend(state, key, owner, ttl_ms)
  end

  defp apply_single(state, {:ratelimit_add, key, window_ms, max, count}) do
    do_ratelimit_add(state, key, window_ms, max, count, nil)
  end

  defp apply_single(state, {:ratelimit_add, key, window_ms, max, count, now_ms}) do
    do_ratelimit_add(state, key, window_ms, max, count, now_ms)
  end

  defp apply_single(state, {:locked_put, key, value, expire_at_ms, owner_ref}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, owner_ref) do
      :ok -> do_put(state, key, value, expire_at_ms)
      {:error, _reason} = error -> error
    end
  end

  defp apply_single(state, {:locked_put_blob_ref, key, encoded_ref, expire_at_ms, owner_ref}) do
    do_locked_put_blob_ref(state, key, encoded_ref, expire_at_ms, owner_ref)
  end

  defp apply_single(state, {:flow_create, _key, attrs}) do
    do_flow_create(state, attrs)
  end

  defp apply_single(state, {:flow_create_many, _key, attrs}) do
    do_flow_create_many(state, attrs)
  end

  defp apply_single(state, {:flow_create_pipeline_batch, _key, attrs}) do
    do_flow_create_pipeline_batch(state, attrs)
  end

  defp apply_single(state, {:flow_named_value_put, _key, attrs}) do
    do_flow_named_value_put(state, attrs)
  end

  defp apply_single(state, {:flow_signal, _key, attrs}) do
    do_flow_signal(state, attrs)
  end

  defp apply_single(state, {:flow_spawn_children, _key, attrs}) do
    do_flow_spawn_children(state, attrs)
  end

  defp apply_single(state, {:flow_claim_due, _key, attrs}) do
    do_flow_claim_due(state, attrs)
  end

  defp apply_single(state, {:flow_extend_lease, _key, attrs}) do
    do_flow_extend_lease(state, attrs)
  end

  defp apply_single(state, {:flow_complete, _key, attrs}) do
    do_flow_complete(state, attrs)
  end

  defp apply_single(state, {:flow_complete_many, _key, attrs}) do
    do_flow_complete_many(state, attrs)
  end

  defp apply_single(state, {:flow_terminal_pipeline_batch, op, _key, attrs})
       when op in [:complete, :retry, :fail, :cancel] do
    do_flow_terminal_pipeline_batch(state, op, attrs)
  end

  defp apply_single(state, {:flow_transition, _key, attrs}) do
    do_flow_transition(state, attrs)
  end

  defp apply_single(state, {:flow_transition_many, _key, attrs}) do
    do_flow_transition_many(state, attrs)
  end

  defp apply_single(state, {:flow_retry, _key, attrs}) do
    do_flow_retry(state, attrs)
  end

  defp apply_single(state, {:flow_retry_many, _key, attrs}) do
    do_flow_retry_many(state, attrs)
  end

  defp apply_single(state, {:flow_fail, _key, attrs}) do
    do_flow_fail(state, attrs)
  end

  defp apply_single(state, {:flow_fail_many, _key, attrs}) do
    do_flow_fail_many(state, attrs)
  end

  defp apply_single(state, {:flow_cancel, _key, attrs}) do
    do_flow_cancel(state, attrs)
  end

  defp apply_single(state, {:flow_cancel_many, _key, attrs}) do
    do_flow_cancel_many(state, attrs)
  end

  defp apply_single(state, {:flow_retention_cleanup, _key, attrs}) do
    do_flow_retention_cleanup(state, attrs)
  end

  defp apply_single(state, {:flow_rewind, _key, attrs}) do
    do_flow_rewind(state, attrs)
  end

  # -- Probabilistic data structure commands in batch/cross_shard_tx --

  defp apply_single(state, {:bloom_create, key, num_bits, num_hashes, prob_meta}) do
    do_prob_command(state, fn ->
      create_bloom_metadata(state, key, num_bits, num_hashes, prob_meta)
    end)
  end

  defp apply_single(state, {:bloom_add, key, element, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "bloom")

      with :ok <- ensure_prob_dir(state) do
        case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
          :ok -> NIF.bloom_file_add(path, element)
          {:error, _reason} = error -> error
        end
      end
    end)
  end

  defp apply_single(state, {:bloom_madd, key, elements, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "bloom")

      with :ok <- ensure_prob_dir(state) do
        case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
          :ok -> NIF.bloom_file_madd(path, elements)
          {:error, _reason} = error -> error
        end
      end
    end)
  end

  defp apply_single(state, {:cms_create, key, width, depth}) do
    do_prob_command(state, fn ->
      create_cms_metadata(state, key, width, depth)
    end)
  end

  defp apply_single(state, {:cms_incrby, key, items}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cms")
      NIF.cms_file_incrby(path, items)
    end)
  end

  defp apply_single(state, {:cms_merge, dst_key, src_keys, weights, create_params}) do
    do_prob_command(state, fn ->
      dst_path = prob_path(state, dst_key, "cms")
      src_paths = cms_source_paths(state, src_keys)

      with :ok <- ensure_prob_dir(state) do
        case maybe_create_cms_merge_dst(state, dst_path, dst_key, create_params) do
          :ok -> NIF.cms_file_merge(dst_path, src_paths, weights)
          {:error, _reason} = error -> error
        end
      end
    end)
  end

  defp apply_single(state, {:cuckoo_create, key, capacity, bucket_size}) do
    do_prob_command(state, fn ->
      create_cuckoo_metadata(state, key, capacity, bucket_size)
    end)
  end

  defp apply_single(state, {:cuckoo_add, key, element, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")

      with :ok <- ensure_prob_dir(state) do
        case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
          :ok -> NIF.cuckoo_file_add(path, element)
          {:error, _reason} = error -> error
        end
      end
    end)
  end

  defp apply_single(state, {:cuckoo_addnx, key, element, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")

      with :ok <- ensure_prob_dir(state) do
        case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
          :ok -> NIF.cuckoo_file_addnx(path, element)
          {:error, _reason} = error -> error
        end
      end
    end)
  end

  defp apply_single(state, {:cuckoo_del, key, element}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")
      NIF.cuckoo_file_del(path, element)
    end)
  end

  defp apply_single(state, {:topk_create, key, k, width, depth, decay}) do
    do_prob_command(state, fn ->
      create_topk_metadata(state, key, k, width, depth, decay)
    end)
  end

  defp apply_single(state, {:topk_add, key, elements}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "topk")
      NIF.topk_file_add_v2(path, elements)
    end)
  end

  defp apply_single(state, {:topk_incrby, key, pairs}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "topk")
      NIF.topk_file_incrby_v2(path, pairs)
    end)
  end

  defp apply_single(_state, unknown_command) do
    require Logger
    Logger.error("StateMachine: unrecognized batch command: #{inspect(unknown_command)}")
    {:error, {:unknown_command, unknown_command}}
  end

  defp apply_put_batch_entries(state, entries) do
    case prepare_apply_blob_command(state, {:put_batch, entries}) do
      {:ok, {:put_blob_batch, prepared_entries}} ->
        apply_put_blob_batch_entries(state, prepared_entries)

      {:ok, {:put_batch, ^entries}} ->
        apply_plain_put_batch_entries(state, entries)

      {:ok, other} ->
        apply_single(state, other)

      {:error, _reason} = error ->
        error
    end
  end

  defp apply_plain_put_batch_entries(state, entries) do
    case Map.get(state, :cross_shard_locks, %{}) do
      locks when map_size(locks) == 0 ->
        if put_batch_fast_path?(state, entries) do
          apply_put_batch_entries_fast(state, entries)
        else
          Enum.map(entries, fn {key, value, expire_at_ms} ->
            do_put(state, key, value, expire_at_ms)
          end)
        end

      _locks ->
        Enum.map(entries, fn {key, value, expire_at_ms} ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          case check_key_lock(state, redis_key, nil) do
            :ok -> do_put(state, key, value, expire_at_ms)
            {:error, :key_locked} -> {:error, :key_locked}
          end
        end)
    end
  end

  defp apply_put_blob_batch_entries(state, entries) do
    with {:ok, prepared_entries} <- prepare_put_blob_batch_entries(state, entries) do
      Enum.map(prepared_entries, fn
        {:value, key, value, expire_at_ms} ->
          do_put(state, key, value, expire_at_ms)

        {:blob_ref, key, encoded_ref, expire_at_ms, materialized} ->
          redis_key = CompoundKey.extract_redis_key(key)

          case check_key_lock(state, redis_key, nil) do
            :ok -> do_put_blob_ref_validated(state, key, encoded_ref, expire_at_ms, materialized)
            {:error, :key_locked} -> {:error, :key_locked}
          end
      end)
    end
  end

  defp prepare_put_blob_batch_entries(state, entries) do
    with {:ok, prepared, refs} <- decode_put_blob_batch_entries(entries),
         :ok <- verify_blob_refs_for_apply(state, refs),
         {:ok, prepared_entries} <- materialize_put_blob_batch_entries(state, prepared) do
      {:ok, prepared_entries}
    end
  end

  defp decode_put_blob_batch_entries(entries) do
    entries
    |> Enum.reduce_while({:ok, [], []}, fn
      {key, value, expire_at_ms, :value}, {:ok, acc, refs}
      when is_binary(key) and is_binary(value) and is_integer(expire_at_ms) ->
        {:cont, {:ok, [{:value, key, value, expire_at_ms} | acc], refs}}

      {key, encoded_ref, expire_at_ms, :blob_ref}, {:ok, acc, refs}
      when is_binary(key) and is_binary(encoded_ref) and is_integer(expire_at_ms) ->
        case BlobRef.decode(encoded_ref) do
          {:ok, ref} ->
            entry = {:blob_ref, key, encoded_ref, expire_at_ms, ref}
            {:cont, {:ok, [entry | acc], [ref | refs]}}

          :error ->
            {:halt, {:error, {:blob_ref_unavailable, :invalid_blob_ref}}}
        end

      _entry, {:ok, _acc, _refs} ->
        {:halt, {:error, :invalid_put_blob_batch_entry}}
    end)
    |> case do
      {:ok, prepared, refs} -> {:ok, Enum.reverse(prepared), Enum.reverse(refs)}
      {:error, _reason} = error -> error
    end
  end

  defp materialize_put_blob_batch_entries(state, prepared) do
    blob_refs =
      Enum.flat_map(prepared, fn
        {:blob_ref, _key, _encoded_ref, _expire_at_ms, ref} -> [ref]
        _entry -> []
      end)

    blob_values = BlobStore.get_many(state.data_dir, state.shard_index, blob_refs)

    prepared
    |> Enum.map_reduce(blob_values, fn
      {:value, key, value, expire_at_ms}, refs ->
        {{:ok, {:value, key, value, expire_at_ms}}, refs}

      {:blob_ref, key, encoded_ref, expire_at_ms, _ref}, [result | refs] ->
        case result do
          {:ok, materialized} ->
            {{:ok, {:blob_ref, key, encoded_ref, expire_at_ms, materialized}}, refs}

          {:error, reason} ->
            {{:error, {:blob_ref_unavailable, reason}}, refs}
        end
    end)
    |> case do
      {results, []} ->
        collect_prepared_blob_results(results)

      {_results, _leftover} ->
        {:error, {:blob_ref_unavailable, :batch_result_mismatch}}
    end
  end

  defp collect_prepared_blob_results(results) do
    Enum.reduce_while(results, {:ok, []}, fn
      {:ok, entry}, {:ok, acc} -> {:cont, {:ok, [entry | acc]}}
      {:error, _reason} = error, {:ok, _acc} -> {:halt, error}
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp verify_blob_refs_for_apply(_state, []), do: :ok

  defp verify_blob_refs_for_apply(state, refs) do
    case BlobStore.verify_many(state.data_dir, state.shard_index, refs) do
      :ok -> :ok
      {:error, reason} -> {:error, {:blob_ref_unavailable, reason}}
    end
  end

  defp do_put_blob_ref_validated(state, key, encoded_ref, expire_at_ms, materialized) do
    maybe_clear_compound_data_structure_for_string_put(state, key)
    raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized)
  end

  defp put_batch_fast_path?(state, entries) do
    PerfToggles.put_batch_apply_fast_path?() and not cross_shard_pending_active?() and
      not standalone_staged_apply?() and
      Enum.all?(entries, fn {key, _value, _expire_at_ms} ->
        put_batch_plain_string_key?(state, key)
      end)
  end

  defp put_batch_plain_string_key?(state, key) do
    if CompoundKey.internal_key?(key) do
      false
    else
      case :ets.lookup(state.ets, CompoundKey.type_key(key)) do
        [] -> true
        _marker -> false
      end
    end
  end

  # Specialized Ra term contract:
  #
  # `{:put_batch, entries}` is homogeneous and write-only. It does not publish
  # temporary ETS `:pending` rows or fill the generic pending-value map because
  # no later command inside the same Ra entry can read the staged values. The
  # append batch is recorded in `:sm_pending_writes`, and
  # `apply_fast_put_pending_locations/5` publishes the final ETS rows only after
  # the NIF returns ordered append locations.
  #
  # If a future compact term needs read-your-own-write inside the same Ra entry,
  # use the generic `{:batch, commands}` machinery or add a dedicated equivalent
  # with rollback, ordering, and mixed-result tests.
  defp apply_put_batch_entries_fast(_state, entries) do
    pending = Process.get(:sm_pending_writes, [])
    pending_values = Process.get(:sm_pending_values, %{})
    mirror_flow_policy? = flow_lmdb_mirror?(nil)
    fast_publish? = fast_put_publish_possible?(pending, pending_values)

    {results, pending} =
      Enum.reduce(entries, {[], pending}, fn
        {key, value, expire_at_ms}, {results, pending_acc} ->
          disk_val = to_disk_binary(value)

          if mirror_flow_policy? and FlowKeys.policy_key?(key) do
            queue_pending_lmdb_mirror_put(key, disk_val, expire_at_ms)
          end

          {
            [:ok | results],
            [{:put, key, disk_val, expire_at_ms} | pending_acc]
          }
      end)

    Process.put(:sm_pending_writes, pending)
    Process.put(:sm_pending_fast_put_batch, fast_publish?)

    Enum.reverse(results)
  end

  defp apply_delete_batch_keys_fast(state, keys) do
    with true <- delete_batch_fast_path?(state),
         {:ok, prepared} <- maybe_prepare_delete_batch_fast(state, keys),
         true <- Process.get(:sm_pending_writes, []) == [],
         true <- Process.get(:sm_pending_values, %{}) == %{} do
      Enum.each(Enum.reverse(prepared), fn {key, prob_path} ->
        queue_pending_delete_fast(key, prob_path)
      end)

      Process.put(:sm_pending_fast_delete_batch, true)

      Enum.map(keys, fn _key -> :ok end)
    else
      _ -> :fallback
    end
  end

  defp fast_put_publish_possible?(pending, pending_values) do
    pending_values == %{} and
      (pending == [] or
         (Process.get(:sm_pending_fast_put_batch) == true and put_only_pending_batch?(pending)))
  end

  defp delete_batch_fast_path?(state) do
    PerfToggles.delete_batch_apply_fast_path?() and not cross_shard_pending_active?() and
      not standalone_staged_apply?() and Map.get(state, :cross_shard_locks, %{}) == %{}
  end

  defp maybe_prepare_delete_batch_fast(state, keys) do
    now_ms = apply_now_ms()

    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
      case :ets.lookup(state.ets, key) do
        [{^key, _value, _expire_at_ms, _lfu, :pending, _offset, _value_size}] ->
          {:halt, :fallback}

        [{^key, nil, _expire_at_ms, _lfu, _file_id, _offset, _value_size}] ->
          {:halt, :fallback}

        [{^key, _value, expire_at_ms, _lfu, _file_id, _offset, _value_size}]
        when expire_at_ms != 0 and expire_at_ms <= now_ms ->
          {:halt, :fallback}

        [{^key, value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}]
        when is_binary(value) ->
          {:cont, {:ok, [{key, prob_file_path_from_delete_value(state, key, value)} | acc]}}

        [] ->
          {:cont, {:ok, [{key, nil} | acc]}}

        _other ->
          {:halt, :fallback}
      end
    end)
  end

  defp prob_file_path_from_delete_value(state, key, value) when is_binary(value) do
    case safe_binary_to_term(value) do
      {:bloom_meta, %{path: path}} -> path
      {:cms_meta, _} -> prob_path(state, key, "cms")
      {:cuckoo_meta, _} -> prob_path(state, key, "cuckoo")
      {:topk_meta, %{path: path}} -> path
      _ -> nil
    end
  end

  defp safe_binary_to_term(value) do
    :erlang.binary_to_term(value, [:safe])
  rescue
    _ -> :not_term
  end

  defp apply_delete_batch_keys(state, keys) do
    case Map.get(state, :cross_shard_locks, %{}) do
      locks when map_size(locks) == 0 ->
        case apply_delete_batch_keys_fast(state, keys) do
          :fallback ->
            Enum.map(keys, fn key -> do_delete(state, key) end)

          results ->
            results
        end

      _locks ->
        Enum.map(keys, fn key ->
          redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

          case check_key_lock(state, redis_key, nil) do
            :ok -> do_delete(state, key)
            {:error, :key_locked} -> {:error, :key_locked}
          end
        end)
    end
  end

  defp apply_compound_batch_put_entries(state, redis_key, entries) do
    cond do
      not compound_put_entries_for_key?(redis_key, entries) ->
        {:error, :compound_batch_cross_key}

      Map.get(state, :cross_shard_locks, %{}) != %{} ->
        compound_batch_lock_checked_results(state, redis_key, entries, fn ->
          do_apply_compound_batch_put_entries_unlocked(state, redis_key, entries)
        end)

      true ->
        case check_key_lock(state, redis_key, nil) do
          :ok ->
            case do_apply_compound_batch_put_entries_unlocked(state, redis_key, entries) do
              :ok -> List.duplicate(:ok, length(entries))
              {:error, _reason} = error -> error
            end

          {:error, :key_locked} = error ->
            List.duplicate(error, length(entries))
        end
    end
  end

  defp do_apply_compound_batch_put_entries_unlocked(state, redis_key, entries) do
    case prepare_apply_blob_command(state, {:compound_batch_put, redis_key, entries}) do
      {:ok, {:compound_blob_batch_put, ^redis_key, blob_entries}} ->
        with {:ok, prepared_entries} <- prepare_compound_blob_batch_entries(state, blob_entries) do
          do_compound_blob_batch_put(state, redis_key, prepared_entries)
        end

      {:ok, {:compound_batch_put, ^redis_key, ^entries}} ->
        do_compound_batch_put(state, redis_key, entries)

      {:ok, _other} ->
        {:error, :invalid_compound_batch_entry}

      {:error, _reason} = error ->
        error
    end
  end

  defp apply_compound_blob_batch_put_entries(state, redis_key, entries) do
    with true <- compound_blob_put_entries_for_key?(redis_key, entries),
         {:ok, prepared_entries} <- prepare_compound_blob_batch_entries(state, entries) do
      cond do
        Map.get(state, :cross_shard_locks, %{}) != %{} ->
          compound_batch_lock_checked_results(state, redis_key, entries, fn ->
            do_compound_blob_batch_put(state, redis_key, prepared_entries)
          end)

        true ->
          case check_key_lock(state, redis_key, nil) do
            :ok ->
              case do_compound_blob_batch_put(state, redis_key, prepared_entries) do
                :ok -> List.duplicate(:ok, length(entries))
                {:error, _reason} = error -> error
              end

            {:error, :key_locked} = error ->
              List.duplicate(error, length(entries))
          end
      end
    else
      false -> {:error, :compound_batch_cross_key}
      {:error, _reason} = error -> error
    end
  end

  defp prepare_compound_blob_batch_entries(state, entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn
      {compound_key, value, expire_at_ms, :value}, {:ok, acc}
      when is_binary(compound_key) and is_binary(value) and is_integer(expire_at_ms) ->
        {:cont, {:ok, [{:value, compound_key, value, expire_at_ms} | acc]}}

      {compound_key, encoded_ref, expire_at_ms, :blob_ref}, {:ok, acc}
      when is_binary(compound_key) and is_binary(encoded_ref) and is_integer(expire_at_ms) ->
        case materialize_blob_ref(state, encoded_ref) do
          {:ok, materialized} ->
            {:cont,
             {:ok, [{:blob_ref, compound_key, encoded_ref, expire_at_ms, materialized} | acc]}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      _entry, {:ok, _acc} ->
        {:halt, {:error, :invalid_compound_blob_batch_entry}}
    end)
    |> case do
      {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
      {:error, _reason} = error -> error
    end
  end

  defp do_compound_blob_batch_put(state, redis_key, prepared_entries) do
    prepared_entries
    |> Enum.reduce_while(:ok, fn
      {:value, compound_key, value, expire_at_ms}, :ok ->
        case do_compound_put(state, redis_key, compound_key, value, expire_at_ms) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end

      {:blob_ref, compound_key, encoded_ref, expire_at_ms, materialized}, :ok ->
        case do_compound_put_blob_ref_validated(
               state,
               redis_key,
               compound_key,
               encoded_ref,
               expire_at_ms,
               materialized
             ) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
    end)
  end

  defp do_compound_put_blob_ref_validated(
         state,
         redis_key,
         compound_key,
         encoded_ref,
         expire_at_ms,
         materialized
       ) do
    result =
      case promoted_compound_path(state, redis_key, compound_key) do
        nil ->
          raw_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms, materialized)

        dedicated_path ->
          do_promoted_compound_put(
            state,
            redis_key,
            compound_key,
            encoded_ref,
            expire_at_ms,
            dedicated_path
          )
      end

    if result == :ok do
      zset_index_put(state, redis_key, compound_key, materialized)
    end

    result
  end

  defp apply_compound_batch_delete_keys(state, redis_key, compound_keys) do
    cond do
      not compound_delete_keys_for_key?(redis_key, compound_keys) ->
        {:error, :compound_batch_cross_key}

      Map.get(state, :cross_shard_locks, %{}) != %{} ->
        compound_batch_lock_checked_results(state, redis_key, compound_keys, fn ->
          do_compound_batch_delete(state, redis_key, compound_keys)
        end)

      true ->
        case check_key_lock(state, redis_key, nil) do
          :ok ->
            case do_compound_batch_delete(state, redis_key, compound_keys) do
              :ok -> List.duplicate(:ok, length(compound_keys))
              {:error, _reason} = error -> error
            end

          {:error, :key_locked} = error ->
            List.duplicate(error, length(compound_keys))
        end
    end
  end

  defp compound_batch_lock_checked_results(state, redis_key, items, fun) do
    case check_key_lock(state, redis_key, nil) do
      :ok ->
        case fun.() do
          :ok -> List.duplicate(:ok, length(items))
          {:error, _reason} = error -> error
        end

      {:error, :key_locked} = error ->
        List.duplicate(error, length(items))
    end
  end

  defp compound_put_entries_for_key?(redis_key, entries) do
    Enum.all?(entries, fn
      {compound_key, _value, _expire_at_ms} when is_binary(compound_key) ->
        CompoundKey.extract_redis_key(compound_key) == redis_key

      _entry ->
        false
    end)
  end

  defp compound_blob_put_entries_for_key?(redis_key, entries) do
    Enum.all?(entries, fn
      {compound_key, _value_or_ref, _expire_at_ms, kind}
      when is_binary(compound_key) and kind in [:value, :blob_ref] ->
        CompoundKey.extract_redis_key(compound_key) == redis_key

      _entry ->
        false
    end)
  end

  defp compound_delete_keys_for_key?(redis_key, compound_keys) do
    Enum.all?(compound_keys, fn
      compound_key when is_binary(compound_key) ->
        CompoundKey.extract_redis_key(compound_key) == redis_key

      _key ->
        false
    end)
  end

  defp maybe_queue_origin_pending_put(state, key, value, expire_at_ms) do
    expected_value = value_for_ets(value, hot_cache_threshold(state))

    case :ets.lookup(state.ets, key) do
      [{^key, ^expected_value, ^expire_at_ms, _lfu, :pending, 0, _vs}]
      when expected_value != nil ->
        queue_pending_put(key, to_disk_binary(value), expire_at_ms)

      _ ->
        :ok
    end
  end

  defp maybe_queue_already_applied_origin_put(
         state,
         key,
         {:put, _key, value, expire_at_ms},
         expected_value,
         expire_at_ms
       ) do
    case :ets.lookup(state.ets, key) do
      [{^key, ^expected_value, ^expire_at_ms, _lfu, :pending, _off, _value_size}] ->
        queue_pending_put(key, to_disk_binary(value), expire_at_ms)

      _ ->
        :ok
    end
  end

  defp maybe_queue_already_applied_origin_put(
         _state,
         _key,
         _inner_cmd,
         _expected_value,
         _expire_at_ms
       ) do
    :ok
  end

  defp apply_origin_async_put(state, key, value, expire_at_ms) do
    expected_value = value_for_ets(value, hot_cache_threshold(state))
    disk_value = to_disk_binary(value)

    case :ets.lookup(state.ets, key) do
      [{^key, ^expected_value, ^expire_at_ms, _lfu, :pending, 0, _vs}]
      when expected_value != nil ->
        queue_origin_async_put(state, key, value, expire_at_ms)

      [{^key, ^expected_value, ^expire_at_ms, _lfu, fid, off, vs}]
      when fid != :pending and valid_cold_location(fid, off, vs) ->
        if origin_cold_put_already_applied?(state, key, fid, off, vs, disk_value) do
          :ok
        else
          apply_single(state, {:put, key, value, expire_at_ms})
        end

      [{^key, _other_value, _other_exp, _lfu, :pending, _off, _vs}] ->
        queue_origin_async_put(state, key, value, expire_at_ms)

      _ ->
        apply_single(state, {:put, key, value, expire_at_ms})
    end
  end

  defp queue_origin_async_put(state, key, value, expire_at_ms) do
    case maybe_externalize_apply_value(state, value) do
      {:ok, :value, value} ->
        queue_pending_put(key, to_disk_binary(value), expire_at_ms)
        :ok

      {:ok, :blob_ref, encoded_ref, materialized_value} ->
        disk_value = to_disk_binary(encoded_ref)
        record_pending_original(state, key)

        unless standalone_staged_apply?() do
          track_keydir_binary_delta(state, key, nil, expire_at_ms)

          :ets.insert(
            state.ets,
            {key, nil, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_value)}
          )
        end

        queue_pending_put_cold(key, disk_value, expire_at_ms, LFU.initial())
        put_pending_value(key, materialized_value, expire_at_ms)
        Process.put(:sm_pending_fast_staged_put_batch, true)
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp origin_cold_put_already_applied?(_state, _key, _fid, _off, value_size, disk_value)
       when value_size != byte_size(disk_value) do
    false
  end

  defp origin_cold_put_already_applied?(state, key, fid, off, _value_size, disk_value) do
    path = sm_file_path(state, fid)

    case read_cold_async(path, off, key) do
      {:ok, ^disk_value} -> true
      _ -> false
    end
  end

  defp origin_command_already_applied?(state, key, inner_cmd, expected_value, expire_at_ms) do
    case :ets.lookup(state.ets, key) do
      [{^key, ^expected_value, ^expire_at_ms, _lfu, _fid, _off, _vs}]
      when expected_value != nil ->
        true

      [{^key, nil, ^expire_at_ms, _lfu, fid, off, vs}]
      when expected_value == nil and valid_cold_location(fid, off, vs) ->
        true

      [{^key, current_value, _current_exp, _lfu, _fid, _off, _vs}] ->
        origin_command_already_in_current_value?(inner_cmd, current_value, expected_value)

      _ ->
        false
    end
  end

  defp origin_replay_decision(
         state,
         key,
         inner_cmd,
         before_value,
         before_expire_at_ms,
         expected_value,
         expire_at_ms
       ) do
    case :ets.lookup(state.ets, key) do
      [{^key, current_value, current_expire_at_ms, _lfu, :pending, _off, _value_size}] ->
        pending_origin_replay_decision(
          inner_cmd,
          current_value,
          current_expire_at_ms,
          before_value,
          before_expire_at_ms,
          expected_value,
          expire_at_ms
        )

      _ ->
        committed_origin_replay_decision(
          state,
          key,
          inner_cmd,
          before_value,
          before_expire_at_ms,
          expected_value,
          expire_at_ms
        )
    end
  end

  defp committed_origin_replay_decision(
         state,
         key,
         inner_cmd,
         before_value,
         before_expire_at_ms,
         expected_value,
         expire_at_ms
       ) do
    case do_get_meta(state, key) do
      {^expected_value, ^expire_at_ms} when expected_value != nil ->
        :already_applied

      {^before_value, ^before_expire_at_ms} when before_value != nil ->
        :apply

      nil when before_value == nil ->
        :apply

      nil when expected_value == nil ->
        :apply_expected

      _other ->
        pending_newer_origin_replay_decision(state, key, inner_cmd, expected_value)
    end
  end

  defp pending_newer_origin_replay_decision(state, key, inner_cmd, expected_value) do
    case :ets.lookup(state.ets, key) do
      [{^key, current_value, current_expire_at_ms, _lfu, :pending, _off, _value_size}] ->
        pending_origin_replay_decision(
          inner_cmd,
          current_value,
          current_expire_at_ms,
          current_value,
          current_expire_at_ms,
          expected_value,
          current_expire_at_ms
        )

      _ ->
        :newer_local_value
    end
  end

  defp pending_origin_replay_decision(
         {:delete, _key},
         current_value,
         current_expire_at_ms,
         before_value,
         before_expire_at_ms,
         nil,
         _expected_expire_at_ms
       )
       when current_value != before_value or current_expire_at_ms != before_expire_at_ms do
    :newer_local_value
  end

  defp pending_origin_replay_decision(
         {:getdel, _key},
         current_value,
         current_expire_at_ms,
         before_value,
         before_expire_at_ms,
         nil,
         _expected_expire_at_ms
       )
       when current_value != before_value or current_expire_at_ms != before_expire_at_ms do
    :newer_local_value
  end

  defp pending_origin_replay_decision(
         {:getset, _key, _new_value},
         current_value,
         current_expire_at_ms,
         _before_value,
         _before_expire_at_ms,
         expected_value,
         expected_expire_at_ms
       )
       when current_value == expected_value and current_expire_at_ms == expected_expire_at_ms do
    :apply_expected
  end

  defp pending_origin_replay_decision(
         _inner_cmd,
         current_value,
         current_expire_at_ms,
         _before_value,
         _before_expire_at_ms,
         expected_value,
         expected_expire_at_ms
       )
       when current_value == expected_value and current_expire_at_ms == expected_expire_at_ms do
    :already_applied
  end

  defp pending_origin_replay_decision(
         inner_cmd,
         current_value,
         _current_expire_at_ms,
         _before_value,
         _before_expire_at_ms,
         expected_value,
         _expected_expire_at_ms
       ) do
    if origin_command_provably_in_current_value?(inner_cmd, current_value, expected_value) do
      :newer_local_value
    else
      # A pending local value has no Raft index attached. If this command type
      # cannot prove that the pending value includes the accepted origin result,
      # materialize the accepted value and let later Ra entries replay in order.
      :apply_expected
    end
  end

  defp apply_origin_checked_expected(
         state,
         key,
         {:getdel, _key},
         before_value,
         nil,
         _expire_at_ms
       ) do
    _ = do_delete(state, key)
    origin_checked_expected_result({:getdel, key}, before_value, nil)
  end

  defp apply_origin_checked_expected(
         state,
         key,
         inner_cmd,
         before_value,
         expected_value,
         expire_at_ms
       ) do
    case expected_value do
      nil ->
        apply_single(state, inner_cmd)

      value ->
        do_put(state, key, value, expire_at_ms)
        origin_checked_expected_result(inner_cmd, before_value, value)
    end
  end

  defp origin_checked_expected_result({:incr, _key, _delta}, _before_value, expected_value) do
    case coerce_integer(expected_value) do
      {:ok, value} -> {:ok, value}
      :error -> :ok
    end
  end

  defp origin_checked_expected_result(
         {:incr_float, _key, _delta},
         _before_value,
         expected_value
       ) do
    case coerce_float(expected_value) do
      {:ok, value} -> {:ok, value}
      :error -> :ok
    end
  end

  defp origin_checked_expected_result({:append, _key, _suffix}, _before_value, expected_value)
       when is_binary(expected_value) do
    {:ok, byte_size(expected_value)}
  end

  defp origin_checked_expected_result({:getset, _key, _new_value}, before_value, _expected_value) do
    before_value
  end

  defp origin_checked_expected_result(
         {:getex, _key, _expire_at_ms},
         _before_value,
         expected_value
       ) do
    expected_value
  end

  defp origin_checked_expected_result(
         {:setrange, _key, _offset, _value},
         _before_value,
         expected_value
       )
       when is_binary(expected_value) do
    {:ok, byte_size(expected_value)}
  end

  defp origin_checked_expected_result(_inner_cmd, _before_value, _expected_value) do
    :ok
  end

  defp origin_command_already_in_current_value?(
         {:incr, _key, delta},
         current_value,
         expected_value
       ) do
    with {:ok, current} <- coerce_integer(current_value),
         {:ok, expected} <- coerce_integer(expected_value) do
      if delta >= 0, do: current >= expected, else: current <= expected
    else
      _ -> true
    end
  end

  defp origin_command_already_in_current_value?(
         {:incr_float, _key, delta},
         current_value,
         expected_value
       ) do
    with {:ok, current} <- coerce_float(current_value),
         {:ok, expected} <- coerce_float(expected_value) do
      if delta >= 0.0, do: current >= expected, else: current <= expected
    else
      _ -> true
    end
  end

  defp origin_command_already_in_current_value?({:append, _key, _suffix}, current_value, expected)
       when is_binary(current_value) and is_binary(expected) do
    String.starts_with?(current_value, expected)
  end

  defp origin_command_already_in_current_value?(_inner_cmd, _current_value, _expected_value) do
    true
  end

  defp origin_command_provably_in_current_value?(
         {:incr, _key, delta},
         current_value,
         expected_value
       ) do
    with {:ok, current} <- coerce_integer(current_value),
         {:ok, expected} <- coerce_integer(expected_value) do
      if delta >= 0, do: current >= expected, else: current <= expected
    else
      _ -> false
    end
  end

  defp origin_command_provably_in_current_value?(
         {:incr_float, _key, delta},
         current_value,
         expected_value
       ) do
    with {:ok, current} <- coerce_float(current_value),
         {:ok, expected} <- coerce_float(expected_value) do
      if delta >= 0.0, do: current >= expected, else: current <= expected
    else
      _ -> false
    end
  end

  defp origin_command_provably_in_current_value?(
         {:append, _key, _suffix},
         current_value,
         expected
       )
       when is_binary(current_value) and is_binary(expected) do
    String.starts_with?(current_value, expected)
  end

  defp origin_command_provably_in_current_value?(
         {:put, _key, _value, _expire_at_ms},
         _current_value,
         _expected_value
       ) do
    true
  end

  # Deletes are materialized as tombstones, not as a value shape. A pending
  # value can never prove that a later DELETE/GETDEL has already reached disk.
  defp origin_command_provably_in_current_value?({:delete, _key}, _current_value, nil), do: false

  defp origin_command_provably_in_current_value?({:getdel, _key}, _current_value, nil), do: false

  defp origin_command_provably_in_current_value?(_inner_cmd, _current_value, _expected_value) do
    false
  end

  defp normalize_stamped_command({:ratelimit_add, key, window_ms, max, count, _legacy_now_ms}) do
    {:ratelimit_add, key, window_ms, max, count}
  end

  defp normalize_stamped_command({:batch, commands}) when is_list(commands) do
    {:batch, Enum.map(commands, &normalize_stamped_command/1)}
  end

  defp normalize_stamped_command({:async, command}) do
    {:async, normalize_stamped_command(command)}
  end

  defp normalize_stamped_command(command), do: command

  defp with_cross_shard_pending_writes(state, fun) do
    init_pending_write_process_state(state)
    Process.put(:sm_cross_shard_pending_writes, [])
    Process.put(:sm_cross_shard_pending_originals, %{})

    try do
      result = fun.()

      case cross_shard_pending_error_result(result) do
        {:error, _reason} = error ->
          rollback_cross_shard_pending_writes(state)
          rollback_pending_writes(state)
          error

        nil ->
          case flush_cross_shard_pending_writes(state) do
            {:ok, flushed_state} ->
              :ok = flush_pending_flow_native_indexes(flushed_state)

              case publish_pending_flow_history_projections(flushed_state) do
                :ok ->
                  observe_pending_lmdb_mirror_enqueue(state, enqueue_pending_lmdb_mirror(state))
                  {result, flushed_state}

                {:error, reason} ->
                  {:error, reason, flushed_state}
              end

            {:error, reason, partial_state, successful_groups} ->
              case compensate_cross_shard_partial_writes(
                     partial_state,
                     successful_groups,
                     Process.get(:sm_cross_shard_pending_originals, %{})
                   ) do
                {:ok, compensated_state} ->
                  rollback_cross_shard_pending_writes(state)
                  rollback_pending_writes(state)
                  {:error, reason, compensated_state}

                {:error, compensation_reason, compensated_state} ->
                  rollback_cross_shard_pending_writes(state)
                  rollback_pending_writes(state)
                  block_release_cursor_for_apply()

                  {:error, {:cross_shard_compensation_failed, compensation_reason},
                   compensated_state}
              end
          end
      end
    rescue
      error ->
        rollback_cross_shard_pending_writes(state)
        rollback_pending_writes(state)
        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        rollback_cross_shard_pending_writes(state)
        rollback_pending_writes(state)
        :erlang.raise(kind, reason, __STACKTRACE__)
    after
      Process.delete(:sm_cross_shard_pending_writes)
      Process.delete(:sm_cross_shard_pending_originals)
      clear_pending_write_process_state()
    end
  end

  defp cross_shard_pending_error_result({:error, _reason} = error), do: error
  defp cross_shard_pending_error_result({:error, reason, _state}), do: {:error, reason}
  defp cross_shard_pending_error_result(_result), do: nil

  defp flush_cross_shard_pending_writes(state) do
    pending =
      :sm_cross_shard_pending_writes
      |> Process.put([])
      |> Enum.reverse()

    flush_cross_shard_pending_writes(state, pending)
  end

  defp flush_cross_shard_pending_writes(state, pending) do
    staged_publish? = standalone_staged_apply?()

    pending
    |> Enum.group_by(&cross_shard_pending_target/1)
    |> Enum.reduce_while({:ok, state, []}, fn {{idx, file_path, file_id, keydir}, entries},
                                              {:ok, acc_state, successful_groups} ->
      batch = Enum.map(entries, &cross_shard_pending_to_batch_entry/1)
      append_result = append_pending_batch(file_path, batch)
      validated_append_result = validate_append_result(batch, append_result)

      case validated_append_result do
        {:ok, locations} ->
          unless staged_publish? do
            apply_cross_shard_pending_locations(keydir, file_id, entries, locations)
          end

          acc_state =
            acc_state
            |> track_cross_shard_append_bytes(
              idx,
              file_path,
              file_id,
              bitcask_record_bytes(batch)
            )
            |> mark_cross_shard_checkpoint_dirty(idx)

          group = {idx, file_path, file_id, keydir, entries, locations}
          {:cont, {:ok, acc_state, [group | successful_groups]}}

        {:error, reason} ->
          {:halt, {:error, {:bitcask_append_failed, reason}, acc_state, successful_groups}}
      end
    end)
    |> case do
      {:ok, flushed_state, successful_groups} ->
        if staged_publish? do
          publish_cross_shard_pending_groups(flushed_state, successful_groups)
        end

        {:ok, flushed_state}

      {:error, _reason, _partial_state, _successful_groups} = error ->
        error
    end
  end

  defp cross_shard_pending_target(
         {:put, idx, keydir, file_path, file_id, _key, _ets, _disk, _exp}
       ),
       do: {idx, file_path, file_id, keydir}

  defp cross_shard_pending_target({:delete, idx, keydir, file_path, file_id, _key}),
    do: {idx, file_path, file_id, keydir}

  defp cross_shard_pending_to_batch_entry(
         {:put, _idx, _keydir, _file_path, _file_id, key, _ets_value, disk_value, expire_at_ms}
       ),
       do: {:put, key, disk_value, expire_at_ms}

  defp cross_shard_pending_to_batch_entry({:delete, _idx, _keydir, _file_path, _file_id, key}),
    do: {:delete, key, nil}

  defp apply_cross_shard_pending_locations(keydir, file_id, entries, locations) do
    Enum.zip(entries, locations)
    |> only_latest_cross_shard_entries()
    |> Enum.each(fn
      {{:put, _idx, ^keydir, _file_path, ^file_id, key, ets_value, _disk_value, exp},
       {:put, offset, value_size}} ->
        try do
          :ets.select_replace(keydir, [
            {
              {key, ets_value, exp, :"$1", :pending, :_, :_},
              [],
              [{{key, ets_value, exp, :"$1", file_id, offset, value_size}}]
            }
          ])
        rescue
          ArgumentError -> :ok
        end

      {{:delete, _idx, ^keydir, _file_path, ^file_id, _key}, {:delete, _offset, _record_size}} ->
        :ok
    end)
  end

  defp publish_cross_shard_pending_groups(state, successful_groups) do
    ref = keydir_binary_ref(state)

    Enum.each(successful_groups, fn {_idx, _file_path, file_id, keydir, entries, locations} ->
      publish_cross_shard_pending_locations(ref, keydir, file_id, entries, locations)
    end)
  end

  defp publish_cross_shard_pending_locations(ref, keydir, file_id, entries, locations) do
    Enum.zip(entries, locations)
    |> only_latest_cross_shard_entries()
    |> Enum.each(fn
      {{:put, idx, ^keydir, _file_path, ^file_id, key, ets_value, _disk_value, exp},
       {:put, offset, value_size}} ->
        track_cross_shard_keydir_binary_publish(ref, keydir, idx, key, ets_value)
        :ets.insert(keydir, {key, ets_value, exp, LFU.initial(), file_id, offset, value_size})

      {{:delete, idx, ^keydir, _file_path, ^file_id, key}, {:delete, _offset, _record_size}} ->
        track_cross_shard_keydir_binary_publish(ref, keydir, idx, key, nil)
        :ets.delete(keydir, key)
    end)
  end

  defp track_cross_shard_keydir_binary_publish(nil, _keydir, _shard_index, _key, _new_value),
    do: :ok

  defp track_cross_shard_keydir_binary_publish(ref, keydir, shard_index, key, new_value) do
    current_bytes =
      case :ets.lookup(keydir, key) do
        [{^key, value, _, _, _, _, _}] -> binary_byte_size(key) + binary_byte_size(value)
        _ -> 0
      end

    new_bytes =
      if is_nil(new_value) do
        0
      else
        binary_byte_size(key) + binary_byte_size(new_value)
      end

    delta = new_bytes - current_bytes
    if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
  end

  defp only_latest_cross_shard_entries(entry_locations) do
    latest =
      entry_locations
      |> Enum.with_index()
      |> Enum.reduce(%{}, fn {{entry, _location}, idx}, acc ->
        Map.put(acc, cross_shard_entry_identity(entry), idx)
      end)

    entry_locations
    |> Enum.with_index()
    |> Enum.flat_map(fn {{entry, location}, idx} ->
      if Map.fetch!(latest, cross_shard_entry_identity(entry)) == idx do
        [{entry, location}]
      else
        []
      end
    end)
  end

  defp cross_shard_entry_identity(
         {:put, _idx, keydir, _file_path, _file_id, key, _ets_value, _disk_value, _expire_at_ms}
       ),
       do: {keydir, key}

  defp cross_shard_entry_identity({:delete, _idx, keydir, _file_path, _file_id, key}),
    do: {keydir, key}

  defp compensate_cross_shard_partial_writes(state, successful_groups, originals) do
    Enum.reduce_while(successful_groups, {:ok, state}, fn group, {:ok, acc_state} ->
      {idx, file_path, file_id, keydir, entries} = cross_shard_successful_group_parts(group)

      case cross_shard_compensation_batch(idx, keydir, file_path, entries, originals) do
        {:ok, []} ->
          {:cont, {:ok, acc_state}}

        {:ok, compensation_batch} ->
          append_result =
            file_path
            |> append_pending_batch(
              compensation_batch,
              batch_contains_delete?(compensation_batch)
            )
            |> then(&validate_append_result(compensation_batch, &1))

          case append_result do
            {:ok, _locations} ->
              compensated_state =
                acc_state
                |> track_cross_shard_append_bytes(
                  idx,
                  file_path,
                  file_id,
                  bitcask_record_bytes(compensation_batch)
                )
                |> mark_cross_shard_checkpoint_dirty(idx)

              {:cont, {:ok, compensated_state}}

            {:error, reason} ->
              compensated_state = mark_cross_shard_checkpoint_dirty(acc_state, idx)
              {:halt, {:error, {:compensation_append_failed, reason}, compensated_state}}
          end

        {:error, reason} ->
          compensated_state = mark_cross_shard_checkpoint_dirty(acc_state, idx)
          {:halt, {:error, reason, compensated_state}}
      end
    end)
  end

  defp cross_shard_successful_group_parts({idx, file_path, file_id, keydir, entries}),
    do: {idx, file_path, file_id, keydir, entries}

  defp cross_shard_successful_group_parts({idx, file_path, file_id, keydir, entries, _locations}),
    do: {idx, file_path, file_id, keydir, entries}

  defp cross_shard_compensation_batch(idx, keydir, file_path, entries, originals) do
    entries
    |> Enum.map(&cross_shard_pending_key/1)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, []}, fn key, {:ok, acc} ->
      original = Map.get(originals, {keydir, key}, {idx, :missing})

      case cross_shard_compensation_batch_entry(key, original, file_path) do
        {:ok, batch_entries} -> {:cont, {:ok, [batch_entries | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, batches} -> {:ok, batches |> Enum.reverse() |> List.flatten()}
      {:error, reason} -> {:error, reason}
    end
  end

  defp cross_shard_pending_key(
         {:put, _idx, _keydir, _file_path, _file_id, key, _ets, _disk, _exp}
       ),
       do: key

  defp cross_shard_pending_key({:delete, _idx, _keydir, _file_path, _file_id, key}), do: key

  defp cross_shard_compensation_batch_entry(key, {_idx, :missing}, _file_path) do
    {:ok, [{:delete, key, nil}]}
  end

  defp cross_shard_compensation_batch_entry(
         key,
         {_idx,
          {:entry, {original_key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size}}},
         _file_path
       )
       when original_key == key and is_binary(value) do
    {:ok, [{:put, key, value, expire_at_ms}]}
  end

  defp cross_shard_compensation_batch_entry(
         key,
         {_idx, {:entry, {original_key, nil, expire_at_ms, _lfu, file_id, offset, _value_size}}},
         file_path
       )
       when original_key == key do
    shard_data_path = Path.dirname(file_path)
    old_path = sm_file_path_from_path(shard_data_path, file_id)

    case ColdRead.pread_at(old_path, offset, key, @cold_read_timeout_ms) do
      {:ok, value} when is_binary(value) ->
        {:ok, [{:put, key, value, expire_at_ms}]}

      {:error, reason} ->
        {:error, {:compensation_read_failed, key, reason}}

      other ->
        {:error, {:compensation_read_failed, key, other}}
    end
  end

  defp cross_shard_compensation_batch_entry(key, original, _file_path),
    do: {:error, {:compensation_original_mismatch, key, original}}

  defp record_cross_shard_pending_original(ctx, key) do
    originals = Process.get(:sm_cross_shard_pending_originals, %{})
    original_key = {ctx.keydir, key}

    if Map.has_key?(originals, original_key) do
      :ok
    else
      original =
        case :ets.lookup(ctx.keydir, key) do
          [entry] -> {:entry, entry}
          [] -> :missing
        end

      Process.put(
        :sm_cross_shard_pending_originals,
        Map.put(originals, original_key, {ctx.index, original})
      )
    end
  end

  defp queue_cross_shard_pending_put(ctx, key, disk_value, expire_at_ms, ets_value) do
    pending = Process.get(:sm_cross_shard_pending_writes, [])

    Process.put(:sm_cross_shard_pending_writes, [
      {:put, ctx.index, ctx.keydir, ctx.active_file_path, ctx.active_file_id, key, ets_value,
       disk_value, expire_at_ms}
      | pending
    ])
  end

  defp queue_cross_shard_pending_delete(ctx, key) do
    pending = Process.get(:sm_cross_shard_pending_writes, [])
    Process.put(:sm_pending_has_delete, true)

    Process.put(:sm_cross_shard_pending_writes, [
      {:delete, ctx.index, ctx.keydir, ctx.active_file_path, ctx.active_file_id, key} | pending
    ])
  end

  defp rollback_cross_shard_pending_writes(state) do
    ref = keydir_binary_ref(state)

    Process.get(:sm_cross_shard_pending_originals, %{})
    |> Enum.each(fn
      {{keydir, key}, {shard_index, {:entry, entry}}} ->
        track_cross_shard_keydir_binary_restore(ref, keydir, shard_index, key, entry)
        :ets.insert(keydir, entry)

      {{keydir, key}, {shard_index, :missing}} ->
        track_cross_shard_keydir_binary_restore(ref, keydir, shard_index, key, nil)
        :ets.delete(keydir, key)
    end)
  end

  defp track_cross_shard_keydir_binary_restore(nil, _keydir, _shard_index, _key, _entry), do: :ok

  defp track_cross_shard_keydir_binary_restore(ref, keydir, shard_index, key, original_entry) do
    current_bytes =
      case :ets.lookup(keydir, key) do
        [{^key, value, _, _, _, _, _}] -> binary_byte_size(key) + binary_byte_size(value)
        _ -> 0
      end

    original_bytes =
      case original_entry do
        {^key, value, _, _, _, _, _} -> binary_byte_size(key) + binary_byte_size(value)
        _ -> 0
      end

    delta = original_bytes - current_bytes
    if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
  end

  # Wraps a block of state machine operations with batched disk writes.
  # Initializes the pending-writes buffer, runs the block, then flushes
  # all accumulated writes in one no-sync NIF call.
  # If the append fails, restores any ETS entries that were replaced with
  # :pending locations and returns the disk error instead of acknowledging
  # success to the caller.
  defp with_pending_writes(state, fun) do
    init_pending_write_process_state(state)
    started_at = System.monotonic_time()

    try do
      result = fun.()

      if pending_write_error_result?(result) do
        rollback_pending_writes(state)
        emit_raft_apply_telemetry(state, started_at, result, :rolled_back)
        result
      else
        flush_result = flush_pending_writes(state)
        emit_raft_apply_telemetry(state, started_at, result, flush_result)

        case flush_result do
          :ok ->
            case publish_pending_flow_history_projections(state) do
              :ok -> result
              {:error, _reason} = error -> error
            end

          {:error, _reason} = error ->
            error
        end
      end
    rescue
      error ->
        rollback_pending_writes(state)
        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        rollback_pending_writes(state)
        :erlang.raise(kind, reason, __STACKTRACE__)
    after
      clear_pending_write_process_state()
    end
  end

  defp init_pending_write_process_state(state) do
    Enum.each(@sm_pending_write_initial_values, fn {key, value} ->
      Process.put(key, value)
    end)

    Process.put(:sm_pending_lmdb_mirror_default_shard, Map.get(state, :shard_index, 0))
  end

  defp clear_pending_write_process_state do
    Enum.each(@sm_pending_write_keys, &Process.delete/1)
  end

  defp pending_write_error_result?({:error, _reason}), do: true
  defp pending_write_error_result?({:error, _reason, _state}), do: true
  defp pending_write_error_result?(_result), do: false

  defp do_flow_create(state, %{id: id} = attrs) do
    partition_key = Map.get(attrs, :partition_key)
    state_key = FlowKeys.state_key(id, partition_key)

    case flow_create_existing_state(state, attrs, state_key) do
      nil ->
        flow_create_apply_new_record(state, attrs, state_key)

      existing ->
        case flow_create_duplicate_result(state, existing, attrs) do
          {:ok, _existing} -> :ok
          {:error, _reason} = error -> error
        end
    end
  end

  defp do_flow_named_value_put(
         state,
         %{id: id, name: name, value: value, partition_key: partition_key} = attrs
       )
       when is_binary(name) and name != "" do
    now_ms = flow_attrs_now_ms(attrs)

    with {:ok, record} <- flow_require_record(state, id, partition_key) do
      refs = flow_record_value_refs(record)
      digest = flow_value_digest(value)
      existing = Map.get(refs, name)
      override? = Map.get(attrs, :override, false)

      cond do
        flow_named_value_same_digest?(existing, digest) ->
          {:ok,
           %{
             ref: Map.fetch!(existing, :ref),
             partition_key: partition_key,
             owner_flow_id: id,
             name: name,
             version: Map.get(existing, :version),
             created: false,
             stored: false
           }}

        not is_nil(existing) and not override? ->
          {:error,
           "ERR flow value #{name} already exists with different digest; use OVERRIDE true"}

        true ->
          version = Map.fetch!(record, :version) + 1

          with {:ok, value_refs} <-
                 flow_named_value_refs(
                   record,
                   %{
                     values: %{name => value},
                     override_values: if(override?, do: [name], else: [])
                   },
                   id,
                   version,
                   partition_key
                 ) do
            next =
              record
              |> Map.put(:version, version)
              |> Map.put(:updated_at_ms, now_ms)
              |> flow_put_record_value_refs(value_refs)

            with :ok <- flow_validate_record_keys(next),
                 :ok <- flow_put_named_record_values(state, next, %{values: %{name => value}}),
                 :ok <- flow_put_state_record(state, FlowKeys.state_key(id, partition_key), next),
                 :ok <- flow_history_put_planned(state, record, next, "value_put", now_ms),
                 :ok <- flow_after_history_put(state, next) do
              entry = Map.fetch!(value_refs, name)

              {:ok,
               %{
                 ref: Map.fetch!(entry, :ref),
                 partition_key: partition_key,
                 owner_flow_id: id,
                 name: name,
                 version: Map.get(entry, :version),
                 created: is_nil(existing),
                 stored: true
               }}
            end
          end
      end
    end
  end

  defp do_flow_named_value_put(_state, _attrs),
    do: {:error, "ERR flow value name must be a non-empty string"}

  defp do_flow_signal(state, %{id: id, signal: signal} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)

    with {:ok, record} <- flow_require_record(state, id, partition_key),
         :ok <- flow_signal_idempotency_check(state, record, attrs),
         :ok <- flow_signal_transition_allowed?(record, attrs),
         {:ok, next} <- flow_signal_next_record(record, attrs, now_ms),
         :ok <- flow_validate_record_keys(next),
         :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_transition_move_indexes(state, [{record, next}]),
         :ok <- flow_put_state_record(state, FlowKeys.state_key(id, partition_key), next),
         :ok <- flow_signal_idempotency_put(state, next, attrs),
         :ok <-
           flow_history_put_planned(state, record, next, "signaled", now_ms, %{
             "signal" => signal
           }),
         :ok <- flow_after_history_put(state, next) do
      :ok
    end
  end

  defp flow_signal_next_record(record, attrs, now_ms) do
    version = Map.fetch!(record, :version) + 1
    id = Map.fetch!(record, :id)
    partition_key = Map.get(record, :partition_key)

    with {:ok, value_refs} <- flow_named_value_refs(record, attrs, id, version, partition_key) do
      transition_to = Map.get(attrs, :transition_to)

      next =
        record
        |> Map.merge(%{
          version: version,
          updated_at_ms: now_ms,
          ttl_ms: nil,
          retention_ttl_ms: Map.get(record, :retention_ttl_ms),
          history_hot_max_events: Map.get(record, :history_hot_max_events),
          history_max_events: Map.get(record, :history_max_events)
        })
        |> flow_put_record_value_refs(value_refs)

      next =
        if is_binary(transition_to) and transition_to != "" do
          Map.merge(next, %{
            state: transition_to,
            next_run_at_ms: Map.get(attrs, :run_at_ms, now_ms),
            lease_owner: nil,
            lease_token: nil,
            lease_deadline_ms: 0
          })
        else
          next
        end

      {:ok, flow_stamp_terminal_retention(next, now_ms)}
    end
  end

  defp flow_signal_transition_allowed?(record, attrs) do
    case Map.get(attrs, :transition_to) do
      nil ->
        :ok

      transition_to when is_binary(transition_to) and transition_to != "" ->
        cond do
          Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) ->
            {:error, "ERR flow is terminal; use FLOW.REWIND"}

          is_nil(Map.get(attrs, :if_state)) ->
            {:error, "ERR flow signal transition requires if_state"}

          not flow_signal_if_state_match?(Map.get(attrs, :if_state), Map.get(record, :state)) ->
            {:error, "ERR flow state mismatch"}

          true ->
            flow_reject_terminal_transition(transition_to)
        end
    end
  end

  defp flow_signal_if_state_match?(states, state) when is_list(states), do: state in states
  defp flow_signal_if_state_match?(state, state), do: true
  defp flow_signal_if_state_match?(_expected, _state), do: false

  defp flow_signal_idempotency_check(state, record, attrs) do
    case Map.get(attrs, :idempotency_key) do
      key when is_binary(key) and key != "" ->
        idem_key =
          FlowKeys.signal_idempotency_key(
            Map.fetch!(record, :id),
            key,
            Map.get(record, :partition_key)
          )

        digest = flow_signal_digest(attrs)

        case sm_store_batch_get(state, [idem_key], &sm_file_path/2) do
          [^digest] -> :ok
          [nil] -> :ok
          [_other] -> {:error, "ERR flow signal idempotency conflict"}
        end

      _ ->
        :ok
    end
  end

  defp flow_signal_idempotency_put(state, record, attrs) do
    case Map.get(attrs, :idempotency_key) do
      key when is_binary(key) and key != "" ->
        idem_key =
          FlowKeys.signal_idempotency_key(
            Map.fetch!(record, :id),
            key,
            Map.get(record, :partition_key)
          )

        raw_put_cold(state, idem_key, flow_signal_digest(attrs), flow_record_expire_at(record))

      _ ->
        :ok
    end
  end

  defp flow_signal_digest(attrs) do
    attrs
    |> Map.take([
      :signal,
      :if_state,
      :transition_to,
      :run_at_ms,
      :values,
      :value_refs,
      :drop_values,
      :override_values
    ])
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp flow_create_apply_new_record(state, attrs, state_key) do
    key_info = flow_create_fast_key_info(attrs, state_key)
    record = flow_create_record(state, attrs)
    plan = flow_create_fast_plan(record, attrs, key_info)

    with :ok <- flow_many_same_state_machine_shard_by_keys?(state, [key_info]),
         :ok <- flow_validate_create_fast_plan_keys(plan) do
      flow_create_many_fast_apply(state, [plan])
    else
      {:error, _reason} = error -> error
      _ -> flow_create_apply_new_record_slow(state, attrs, state_key, record)
    end
  end

  defp flow_create_apply_new_record_slow(state, attrs, state_key, record) do
    with :ok <- flow_validate_record_keys(record),
         :ok <- flow_put_record_values(state, record, attrs),
         :ok <-
           flow_put_new_state_record(
             state,
             state_key,
             record
           ),
         :ok <- flow_due_put(state, record),
         :ok <- flow_index_put(state, record),
         :ok <- flow_history_put(state, record, "created", Map.get(record, :created_at_ms)),
         :ok <- flow_history_trim(state, record) do
      :ok
    end
  end

  defp do_flow_create_many(state, %{records: [_ | _] = attrs_list}) do
    with :ok <- flow_many_partitions_valid?(state, attrs_list),
         :ok <- flow_create_many_unique?(attrs_list),
         {:ok, _records, new_plans} <- flow_create_many_prepare(state, attrs_list),
         :ok <- flow_create_many_apply(state, new_plans) do
      :ok
    end
  end

  defp do_flow_create_many(_state, _attrs),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp do_flow_create_pipeline_batch(state, %{records: [_ | _] = attrs_list}) do
    case flow_create_pipeline_batch_fast_prepare(state, attrs_list) do
      {:ok, plans} ->
        case flow_create_many_fast_apply(state, plans) do
          :ok -> List.duplicate(:ok, length(attrs_list))
          {:error, _reason} = error -> List.duplicate(error, length(attrs_list))
        end

      :fallback ->
        {results, plans} = flow_create_pipeline_batch_prepare(state, attrs_list)

        if plans == [] do
          results
        else
          case flow_create_many_apply(state, plans) do
            :ok ->
              results

            {:error, _reason} = error ->
              Enum.map(results, fn
                :ok -> error
                other -> other
              end)
          end
        end
    end
  end

  defp do_flow_create_pipeline_batch(_state, _attrs),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp flow_create_pipeline_batch_fast_prepare(state, attrs_list) do
    if Enum.any?(attrs_list, &Map.get(&1, :idempotent, false)) do
      :fallback
    else
      with :ok <- flow_many_partition_keys_present?(attrs_list),
           key_infos = flow_create_fast_key_infos(attrs_list),
           :ok <- flow_many_same_state_machine_shard_by_keys?(state, key_infos),
           :ok <- flow_create_many_unique?(attrs_list),
           {:ok, plans} <- flow_create_non_idempotent_many_prepare(state, attrs_list, key_infos) do
        {:ok, plans}
      else
        _ -> :fallback
      end
    end
  end

  defp flow_create_fast_key_infos(attrs_list) do
    Enum.map(attrs_list, fn attrs ->
      id = Map.fetch!(attrs, :id)
      partition_key = Map.get(attrs, :partition_key)
      tag = FlowKeys.tag(partition_key)

      %{
        partition_key: partition_key,
        tag: tag,
        state_key: flow_state_key_with_tag(tag, id)
      }
    end)
  end

  defp flow_create_fast_key_info(attrs, state_key) do
    partition_key = Map.get(attrs, :partition_key)

    %{
      partition_key: partition_key,
      tag: FlowKeys.tag(partition_key),
      state_key: state_key
    }
  end

  defp flow_many_same_state_machine_shard_by_keys?(
         %{instance_ctx: ctx, shard_index: shard_index},
         key_infos
       )
       when is_map(ctx) do
    if Enum.all?(key_infos, fn %{state_key: key} -> Router.shard_for(ctx, key) == shard_index end) do
      :ok
    else
      {:error, "ERR flow batch crosses shards"}
    end
  rescue
    _ -> :ok
  end

  defp flow_many_same_state_machine_shard_by_keys?(_state, _key_infos), do: :ok

  defp flow_create_non_idempotent_many_prepare(state, attrs_list, key_infos) do
    keys = Enum.map(key_infos, & &1.state_key)

    if Enum.any?(flow_state_keys_present(state, keys), & &1) do
      {:error, "ERR flow already exists"}
    else
      attrs_list
      |> Enum.zip(key_infos)
      |> Enum.reduce_while({:ok, []}, fn {%{id: _id} = attrs, key_info}, {:ok, acc} ->
        record = flow_create_record(state, attrs)
        plan = flow_create_fast_plan(record, attrs, key_info)

        case flow_validate_create_fast_plan_keys(plan) do
          :ok -> {:cont, {:ok, [plan | acc]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
      |> case do
        {:ok, plans} -> {:ok, Enum.reverse(plans)}
        {:error, _reason} = error -> error
      end
    end
  end

  defp flow_create_fast_plan(record, attrs, %{tag: tag, state_key: state_key}) do
    partition_key = Map.get(record, :partition_key)
    id = Map.fetch!(record, :id)
    type = Map.fetch!(record, :type)
    flow_state = Map.fetch!(record, :state)
    score = Map.get(record, :updated_at_ms, 0)
    history_key = flow_history_key_with_tag(tag, id)

    %{
      record: record,
      attrs: attrs,
      partition_key: partition_key,
      tag: tag,
      state_key: state_key,
      history_key: history_key,
      state_index_key: flow_state_index_key_with_tag(tag, type, flow_state),
      state_index_score: score,
      due_key: flow_create_fast_due_key(record, tag, type, flow_state),
      due_any_key: flow_create_fast_due_any_key(record, tag, type),
      running_index_entries: flow_create_fast_running_index_entries(record, tag, type),
      metadata_index_entries: flow_metadata_index_entries_with_tag(record, tag)
    }
  end

  defp flow_create_fast_due_key(%{next_run_at_ms: nil}, _tag, _type, _flow_state), do: nil

  defp flow_create_fast_due_key(%{priority: priority}, tag, type, flow_state) do
    flow_due_key_with_tag(tag, type, flow_state, priority)
  end

  defp flow_create_fast_due_any_key(%{next_run_at_ms: nil}, _tag, _type), do: nil

  defp flow_create_fast_due_any_key(%{priority: priority}, tag, type) do
    if flow_due_any_index_enabled?() do
      flow_due_any_key_with_tag(tag, type, priority)
    else
      nil
    end
  end

  defp flow_create_fast_running_index_entries(%{state: "running"} = record, tag, type) do
    lease_score = Map.get(record, :lease_deadline_ms, 0)

    [
      {flow_inflight_index_key_with_tag(tag, type), lease_score},
      {flow_worker_index_key_with_tag(tag, Map.get(record, :lease_owner, "")), lease_score}
    ]
  end

  defp flow_create_fast_running_index_entries(_record, _tag, _type), do: []

  defp flow_validate_create_fast_plan_keys(%{
         record: record,
         state_key: state_key,
         history_key: history_key,
         state_index_key: state_index_key,
         due_key: due_key,
         due_any_key: due_any_key,
         running_index_entries: running_index_entries,
         metadata_index_entries: metadata_index_entries
       }) do
    with :ok <- flow_validate_key_size(state_key),
         :ok <- flow_validate_key_size(history_key),
         :ok <- flow_validate_key_size(state_index_key),
         :ok <-
           flow_validate_key_size(
             FlowKeys.stream_entry_key_from_history_key(
               history_key,
               "18446744073709551615-18446744073709551615"
             )
           ),
         :ok <- flow_validate_create_fast_due_key(due_key),
         :ok <- flow_validate_create_fast_due_key(due_any_key),
         :ok <- flow_validate_create_fast_index_entries(running_index_entries),
         :ok <- flow_validate_create_fast_metadata_entries(metadata_index_entries) do
      if byte_size(Map.fetch!(record, :id)) <= Router.max_key_size() do
        :ok
      else
        {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
      end
    end
  end

  defp flow_validate_create_fast_due_key(nil), do: :ok
  defp flow_validate_create_fast_due_key(key), do: flow_validate_key_size(key)

  defp flow_validate_create_fast_index_entries(entries) do
    Enum.reduce_while(entries, :ok, fn {key, _score}, :ok ->
      case flow_validate_key_size(key) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_validate_create_fast_metadata_entries(entries) do
    Enum.reduce_while(entries, :ok, fn {key, _id, _score}, :ok ->
      case flow_validate_key_size(key) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_create_pipeline_batch_prepare(state, attrs_list) do
    {results, plans, _seen} =
      Enum.reduce(attrs_list, {[], [], MapSet.new()}, fn attrs, {results, plans, seen} ->
        key = FlowKeys.state_key(Map.get(attrs, :id), Map.get(attrs, :partition_key))

        if MapSet.member?(seen, key) do
          {[{:error, "ERR flow already exists"} | results], plans, seen}
        else
          case flow_create_many_prepare(state, [attrs]) do
            {:ok, _records, new_plans} ->
              {[:ok | results], Enum.reverse(new_plans) ++ plans, MapSet.put(seen, key)}

            {:error, _reason} = error ->
              {[error | results], plans, seen}
          end
        end
      end)

    {Enum.reverse(results), Enum.reverse(plans)}
  end

  defp flow_create_existing_state(state, %{idempotent: true}, state_key) do
    flow_read_record_by_key(state, state_key)
  end

  defp flow_create_existing_state(state, _attrs, state_key) do
    if flow_state_key_present?(state, state_key), do: :present, else: nil
  end

  defp do_flow_spawn_children(
         state,
         %{id: parent_id, partition_key: partition_key, children: [_ | _] = children} = attrs
       ) do
    now_ms = flow_attrs_now_ms(attrs)

    with {:ok, parent} <- flow_require_record(state, parent_id, partition_key),
         :ok <- flow_require_parent_partition(parent, partition_key),
         child_attrs = flow_spawn_child_attrs(parent, children),
         :ok <- flow_many_partitions_valid?(state, child_attrs),
         :ok <- flow_create_many_unique?(child_attrs),
         {:ok, group_state} <- flow_child_group_spawn_state(parent, attrs, child_attrs) do
      case group_state do
        :idempotent ->
          :ok

        :new ->
          with :ok <- flow_require_expected_state(parent, Map.get(attrs, :from_state)),
               :ok <- flow_require_fencing_token(parent, Map.fetch!(attrs, :fencing_token)),
               :ok <- flow_require_transition_lease(parent, Map.get(attrs, :lease_token)),
               :ok <- flow_require_active_parent(parent),
               :ok <- flow_require_spawn_wait_state(parent, attrs),
               {:ok, _child_records, child_plans} <- flow_create_many_prepare(state, child_attrs),
               {:ok, next_parent} <- flow_prepare_spawn_parent(parent, attrs, child_attrs, now_ms),
               :ok <- flow_validate_record_keys(next_parent),
               :ok <-
                 flow_apply_parent_update(state, parent, next_parent, "children_spawned", now_ms),
               :ok <- flow_create_many_apply(state, child_plans) do
            :ok
          end
      end
    end
  end

  defp do_flow_spawn_children(_state, _attrs),
    do: {:error, "ERR flow children must be a non-empty list"}

  defp do_flow_cross_spawn_children(
         state,
         %{id: parent_id, partition_key: partition_key, children: [_ | _] = children} = attrs
       ) do
    parent_state = cross_shard_state_for_key(state, FlowKeys.state_key(parent_id, partition_key))
    now_ms = flow_attrs_now_ms(attrs)

    with {:ok, parent} <- flow_require_record(parent_state, parent_id, partition_key),
         :ok <- flow_require_parent_partition(parent, partition_key),
         child_attrs = flow_spawn_child_attrs(parent, children),
         :ok <- flow_many_partition_keys_present?(child_attrs),
         :ok <- flow_create_many_unique?(child_attrs),
         {:ok, group_state} <- flow_child_group_spawn_state(parent, attrs, child_attrs) do
      case group_state do
        :idempotent ->
          :ok

        :new ->
          with :ok <- flow_require_expected_state(parent, Map.get(attrs, :from_state)),
               :ok <- flow_require_fencing_token(parent, Map.fetch!(attrs, :fencing_token)),
               :ok <- flow_require_transition_lease(parent, Map.get(attrs, :lease_token)),
               :ok <- flow_require_active_parent(parent),
               :ok <- flow_require_spawn_wait_state(parent, attrs),
               {:ok, child_apply_groups} <- flow_cross_create_many_prepare(state, child_attrs),
               {:ok, next_parent} <- flow_prepare_spawn_parent(parent, attrs, child_attrs, now_ms),
               :ok <- flow_validate_record_keys(next_parent),
               :ok <-
                 flow_apply_parent_update(
                   parent_state,
                   parent,
                   next_parent,
                   "children_spawned",
                   now_ms
                 ),
               :ok <- flow_cross_create_many_apply(child_apply_groups) do
            :ok
          end
      end
    end
  end

  defp do_flow_cross_spawn_children(_state, _attrs),
    do: {:error, "ERR flow children must be a non-empty list"}

  defp flow_cross_create_many_prepare(state, attrs_list) do
    attrs_list
    |> Enum.group_by(fn attrs ->
      key = FlowKeys.state_key(Map.fetch!(attrs, :id), Map.fetch!(attrs, :partition_key))
      cross_shard_state_for_key(state, key)
    end)
    |> Enum.reduce_while({:ok, []}, fn {child_state, shard_attrs}, {:ok, acc} ->
      with :ok <- flow_many_partitions_valid?(child_state, shard_attrs),
           {:ok, _records, plans} <- flow_create_many_prepare(child_state, shard_attrs) do
        {:cont, {:ok, [{child_state, plans} | acc]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, groups} -> {:ok, Enum.reverse(groups)}
      {:error, _reason} = error -> error
    end
  end

  defp flow_cross_create_many_apply(groups) do
    Enum.reduce_while(groups, :ok, fn {child_state, plans}, :ok ->
      case flow_create_many_apply(child_state, plans) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_create_record(state, %{id: id, type: type, state: flow_state} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    run_at_ms = Map.get(attrs, :run_at_ms, now_ms)
    priority = Map.get(attrs, :priority, 0)
    retention = flow_retention_for_create(state, attrs)

    flow_create_record_with_retention(
      attrs,
      id,
      type,
      flow_state,
      now_ms,
      run_at_ms,
      priority,
      retention
    )
  end

  defp flow_create_record_cached_retention(state, attrs, retention_cache) do
    key = flow_create_retention_cache_key(attrs)

    case Map.fetch(retention_cache, key) do
      {:ok, retention} ->
        {flow_create_record_with_resolved_retention(attrs, retention), retention_cache}

      :error ->
        retention = flow_retention_for_create(state, attrs)

        {flow_create_record_with_resolved_retention(attrs, retention),
         Map.put(retention_cache, key, retention)}
    end
  end

  defp flow_create_record_with_resolved_retention(
         %{id: id, type: type, state: flow_state} = attrs,
         retention
       ) do
    now_ms = flow_attrs_now_ms(attrs)
    run_at_ms = Map.get(attrs, :run_at_ms, now_ms)
    priority = Map.get(attrs, :priority, 0)

    flow_create_record_with_retention(
      attrs,
      id,
      type,
      flow_state,
      now_ms,
      run_at_ms,
      priority,
      retention
    )
  end

  defp flow_create_record_with_retention(
         attrs,
         id,
         type,
         flow_state,
         now_ms,
         run_at_ms,
         priority,
         retention
       ) do
    partition_key = Map.get(attrs, :partition_key)

    %{
      id: id,
      type: type,
      state: flow_state,
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: now_ms,
      updated_at_ms: now_ms,
      next_run_at_ms: run_at_ms,
      priority: priority,
      ttl_ms: nil,
      retention_ttl_ms: Map.fetch!(retention, :ttl_ms),
      terminal_retention_until_ms: nil,
      history_hot_max_events: Map.fetch!(retention, :history_hot_max_events),
      history_max_events: Map.fetch!(retention, :history_max_events),
      partition_key: partition_key,
      payload_ref: flow_value_ref(attrs, :payload, id, 1, partition_key),
      value_refs: flow_new_named_value_refs(attrs, id, 1, partition_key),
      parent_flow_id: Map.get(attrs, :parent_flow_id),
      parent_partition_key: Map.get(attrs, :parent_partition_key),
      root_flow_id: Map.get(attrs, :root_flow_id) || id,
      correlation_id: Map.get(attrs, :correlation_id),
      result_ref: nil,
      error_ref: nil,
      lease_owner: nil,
      lease_token: nil,
      lease_deadline_ms: 0,
      run_state: nil,
      child_groups: %{}
    }
    |> flow_stamp_terminal_retention(now_ms)
  end

  defp flow_create_retention_cache_key(attrs) do
    {
      Map.get(attrs, :type),
      Map.get(attrs, :state),
      Map.get(attrs, :retention_ttl_ms),
      Map.get(attrs, :history_hot_max_events),
      Map.get(attrs, :history_max_events)
    }
  end

  defp flow_require_parent_partition(parent, partition_key) do
    if Map.get(parent, :partition_key) == partition_key do
      :ok
    else
      {:error, "ERR flow parent partition mismatch"}
    end
  end

  defp flow_require_active_parent(%{state: state}) do
    if Ferricstore.Flow.LMDB.terminal_state?(state) do
      {:error, "ERR flow parent is terminal"}
    else
      :ok
    end
  end

  defp flow_require_spawn_wait_state(_parent, %{wait: wait, wait_state: wait_state})
       when wait in [:all, :any] do
    if is_binary(wait_state) and wait_state != "" do
      :ok
    else
      {:error, "ERR flow wait_state is required when waiting for children"}
    end
  end

  defp flow_require_spawn_wait_state(_parent, _attrs), do: :ok

  defp flow_child_group_spawn_state(parent, attrs, child_attrs) do
    group_id = Map.fetch!(attrs, :group_id)
    requested_hash = flow_child_group_request_hash(attrs, child_attrs)

    case Map.get(flow_child_groups(parent), group_id) do
      nil ->
        {:ok, :new}

      %{"request_hash" => ^requested_hash} ->
        {:ok, :idempotent}

      _existing ->
        {:error, "ERR flow child group idempotency conflict"}
    end
  end

  defp flow_spawn_child_attrs(parent, children) do
    root_flow_id = Map.get(parent, :root_flow_id) || Map.fetch!(parent, :id)
    parent_id = Map.fetch!(parent, :id)
    partition_key = Map.get(parent, :partition_key)

    Enum.map(children, fn attrs ->
      attrs
      |> Map.put(:parent_flow_id, parent_id)
      |> Map.put(:parent_partition_key, partition_key)
      |> Map.put(:root_flow_id, root_flow_id)
      |> Map.put_new(:partition_key, partition_key)
    end)
  end

  defp flow_prepare_spawn_parent(parent, attrs, child_attrs, now_ms) do
    group = flow_new_child_group(attrs, child_attrs)
    groups = Map.put(flow_child_groups(parent), Map.fetch!(attrs, :group_id), group)
    state = flow_spawn_parent_state(parent, attrs)

    next =
      parent
      |> Map.merge(%{
        state: state,
        version: Map.fetch!(parent, :version) + 1,
        updated_at_ms: now_ms,
        next_run_at_ms: nil,
        ttl_ms: nil,
        lease_owner: nil,
        lease_token: nil,
        lease_deadline_ms: 0,
        child_groups: groups
      })
      |> flow_stamp_terminal_retention(now_ms)

    {:ok, next}
  end

  defp flow_spawn_parent_state(_parent, %{wait: :none, exhaust_to: %{"success" => state}}),
    do: state

  defp flow_spawn_parent_state(_parent, %{wait: wait, wait_state: wait_state})
       when wait in [:all, :any] and is_binary(wait_state) and wait_state != "",
       do: wait_state

  defp flow_spawn_parent_state(parent, _attrs), do: Map.fetch!(parent, :state)

  defp flow_new_child_group(attrs, child_attrs) do
    children =
      child_attrs
      |> Enum.map(fn %{id: id} -> {id, "running"} end)
      |> Map.new()

    child_partitions =
      child_attrs
      |> Enum.map(fn %{id: id, partition_key: child_partition} -> {id, child_partition} end)
      |> Map.new()

    resolved =
      case Map.fetch!(attrs, :wait) do
        :none -> "success"
        :all -> nil
        :any -> nil
      end

    %{
      "wait" => Atom.to_string(Map.fetch!(attrs, :wait)),
      "on_child_failed" => Atom.to_string(Map.fetch!(attrs, :on_child_failed)),
      "on_parent_closed" => Atom.to_string(Map.fetch!(attrs, :on_parent_closed)),
      "exhaust_to" => Map.fetch!(attrs, :exhaust_to),
      "request_hash" => flow_child_group_request_hash(attrs, child_attrs),
      "children" => children,
      "child_partitions" => child_partitions,
      "summary" => %{
        "total" => map_size(children),
        "completed" => 0,
        "failed" => 0,
        "cancelled" => 0
      },
      "results" => %{},
      "resolved" => resolved
    }
  end

  defp flow_child_group_request_hash(attrs, child_attrs) do
    request = %{
      wait: Map.fetch!(attrs, :wait),
      wait_state: Map.get(attrs, :wait_state),
      on_child_failed: Map.fetch!(attrs, :on_child_failed),
      on_parent_closed: Map.fetch!(attrs, :on_parent_closed),
      exhaust_to: Map.fetch!(attrs, :exhaust_to),
      children:
        child_attrs
        |> Enum.map(&flow_child_group_request_child/1)
        |> Enum.sort_by(fn child ->
          {Map.fetch!(child, :id), Map.fetch!(child, :partition_key)}
        end)
    }

    :crypto.hash(:sha256, :erlang.term_to_binary(request))
    |> Base.encode16(case: :lower)
  end

  defp flow_child_group_request_child(attrs) do
    attrs
    |> Map.take([
      :id,
      :type,
      :state,
      :partition_key,
      :run_at_ms,
      :priority,
      :retention_ttl_ms,
      :history_hot_max_events,
      :history_max_events,
      :correlation_id,
      :payload_ref,
      :payload
    ])
    |> Map.put(:payload_hash, flow_child_group_payload_hash(Map.get(attrs, :payload)))
    |> Map.delete(:payload)
  end

  defp flow_child_group_payload_hash(nil), do: nil

  defp flow_child_group_payload_hash(payload) do
    :crypto.hash(:sha256, :erlang.term_to_binary(payload))
    |> Base.encode16(case: :lower)
  end

  defp flow_child_groups(record) do
    case Map.get(record, :child_groups) do
      groups when is_map(groups) -> groups
      _ -> %{}
    end
  end

  defp flow_apply_parent_update(state, record, next, event, now_ms) do
    plans = [{record, next}]

    with :ok <- flow_transition_move_indexes(state, plans),
         :ok <-
           flow_put_state_record(
             state,
             FlowKeys.state_key(next.id, Map.get(next, :partition_key)),
             next
           ),
         :ok <- flow_history_put_planned(state, record, next, event, now_ms),
         :ok <- flow_after_history_put(state, next) do
      :ok
    end
  end

  defp flow_retention_for_create(state, attrs) do
    flow_policy = flow_read_policy(state, Map.get(attrs, :type))

    override =
      %{}
      |> maybe_put_retention_override(:ttl_ms, Map.get(attrs, :retention_ttl_ms))
      |> maybe_put_retention_override(
        :history_hot_max_events,
        Map.get(attrs, :history_hot_max_events)
      )
      |> maybe_put_retention_override(
        :history_max_events,
        Map.get(attrs, :history_max_events)
      )

    RetryPolicy.resolve_retention(flow_policy, Map.get(attrs, :state), override)
  end

  defp maybe_put_retention_override(map, _key, nil), do: map
  defp maybe_put_retention_override(map, key, value), do: Map.put(map, key, value)

  defp flow_stamp_terminal_retention(
         %{state: state, terminal_retention_until_ms: nil} = record,
         _now_ms
       )
       when state != "completed" and state != "failed" and state != "cancelled" do
    record
  end

  defp flow_stamp_terminal_retention(record, now_ms) do
    if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      retention_ttl_ms = Map.get(record, :retention_ttl_ms)

      if is_integer(retention_ttl_ms) and retention_ttl_ms > 0 do
        retention_start_ms = max(now_ms, apply_now_ms())
        Map.put(record, :terminal_retention_until_ms, retention_start_ms + retention_ttl_ms)
      else
        Map.put(record, :terminal_retention_until_ms, nil)
      end
    else
      Map.put(record, :terminal_retention_until_ms, nil)
    end
  end

  defp flow_create_duplicate_result(state, existing, %{idempotent: true} = attrs) do
    if flow_create_idempotent_match?(state, existing, attrs) do
      {:ok, existing}
    else
      {:error, "ERR flow idempotency conflict"}
    end
  end

  defp flow_create_duplicate_result(_state, _existing, _attrs),
    do: {:error, "ERR flow already exists"}

  defp flow_value_ref(attrs, kind, id, version, partition_key, existing_ref \\ nil) do
    cond do
      Map.has_key?(attrs, kind) ->
        FlowKeys.value_key(id, kind, version, partition_key)

      ref = Map.get(attrs, flow_value_ref_field(kind)) ->
        ref

      true ->
        existing_ref
    end
  end

  defp flow_value_ref_field(:payload), do: :payload_ref
  defp flow_value_ref_field(:result), do: :result_ref
  defp flow_value_ref_field(:error), do: :error_ref

  defp flow_new_named_value_refs(attrs, id, version, partition_key) do
    case flow_named_value_refs(%{}, attrs, id, version, partition_key) do
      {:ok, refs} -> refs
      {:error, _reason} -> %{}
    end
  end

  defp flow_named_value_refs(record_or_refs, attrs, id, _version, partition_key) do
    values = flow_named_values(Map.get(attrs, :values))

    refs =
      record_or_refs
      |> flow_record_value_refs()
      |> flow_drop_named_value_refs(Map.get(attrs, :drop_values))
      |> flow_merge_external_value_refs(Map.get(attrs, :value_refs))

    value_names = flow_named_value_names(values)
    overrides = flow_named_value_name_set(Map.get(attrs, :override_values))

    Enum.reduce_while(value_names, {:ok, refs}, fn name, {:ok, acc} ->
      value = Map.fetch!(values, name)
      digest = flow_value_digest(value)
      existing = Map.get(acc, name)

      cond do
        flow_named_value_same_digest?(existing, digest) ->
          {:cont, {:ok, acc}}

        not is_nil(existing) and not MapSet.member?(overrides, name) ->
          {:halt,
           {:error,
            "ERR flow value #{name} already exists with different digest; use OVERRIDE true"}}

        true ->
          next_version = flow_named_value_next_version(existing)
          ref = FlowKeys.value_key(id <> ":" <> name, :shared, next_version, partition_key)

          {:cont, {:ok, Map.put(acc, name, %{ref: ref, version: next_version, digest: digest})}}
      end
    end)
  end

  defp flow_record_value_refs(%{} = record_or_refs) do
    record_or_refs
    |> Map.get(:value_refs, record_or_refs)
    |> flow_normalize_value_refs()
  end

  defp flow_record_value_refs(_record), do: %{}

  defp flow_put_record_value_refs(record, refs) when is_map(refs) and map_size(refs) > 0,
    do: Map.put(record, :value_refs, refs)

  defp flow_put_record_value_refs(record, _refs), do: Map.delete(record, :value_refs)

  defp flow_normalize_value_refs(refs) when is_map(refs) do
    Enum.reduce(refs, %{}, fn
      {name, %{ref: ref} = entry}, acc when is_binary(name) and is_binary(ref) and ref != "" ->
        Map.put(acc, name, %{
          ref: ref,
          version: flow_named_value_version(Map.get(entry, :version)),
          digest: flow_named_value_digest_value(Map.get(entry, :digest))
        })

      {name, %{"ref" => ref} = entry}, acc
      when is_binary(name) and is_binary(ref) and ref != "" ->
        Map.put(acc, name, %{
          ref: ref,
          version: flow_named_value_version(Map.get(entry, "version")),
          digest: flow_named_value_digest_value(Map.get(entry, "digest"))
        })

      {name, ref}, acc when is_binary(name) and is_binary(ref) and ref != "" ->
        Map.put(acc, name, %{ref: ref, version: nil, digest: nil})

      _entry, acc ->
        acc
    end)
  end

  defp flow_normalize_value_refs(refs) when is_binary(refs) do
    case Jason.decode(refs) do
      {:ok, decoded} -> flow_normalize_value_refs(decoded)
      _ -> %{}
    end
  end

  defp flow_normalize_value_refs(_refs), do: %{}

  defp flow_merge_external_value_refs(refs, external_refs) when is_map(external_refs) do
    Map.merge(refs, flow_normalize_value_refs(external_refs))
  end

  defp flow_merge_external_value_refs(refs, external_refs) when is_list(external_refs) do
    Map.merge(refs, flow_normalize_value_refs(Map.new(external_refs)))
  rescue
    _ -> refs
  end

  defp flow_merge_external_value_refs(refs, _external_refs), do: refs

  defp flow_drop_named_value_refs(refs, drops) do
    drops
    |> flow_named_value_name_set()
    |> Enum.reduce(refs, &Map.delete(&2, &1))
  end

  defp flow_named_value_names(values) when is_map(values) do
    values
    |> Map.keys()
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  defp flow_named_value_names(values) when is_list(values) do
    values
    |> Enum.flat_map(fn
      {name, _value} when is_binary(name) and name != "" -> [name]
      _other -> []
    end)
    |> Enum.uniq()
  end

  defp flow_named_value_names(_values), do: []

  defp flow_named_values(values) when is_map(values) do
    values
    |> Enum.reduce(%{}, fn
      {name, value}, acc when is_binary(name) and name != "" -> Map.put(acc, name, value)
      _other, acc -> acc
    end)
  end

  defp flow_named_values(values) when is_list(values) do
    values
    |> Enum.reduce(%{}, fn
      {name, value}, acc when is_binary(name) and name != "" -> Map.put(acc, name, value)
      _other, acc -> acc
    end)
  end

  defp flow_named_values(_values), do: %{}

  defp flow_named_value_name_set(nil), do: MapSet.new()

  defp flow_named_value_name_set(value) when is_binary(value) and value != "",
    do: MapSet.new([value])

  defp flow_named_value_name_set(values) when is_list(values) do
    values
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> MapSet.new()
  end

  defp flow_named_value_name_set(_values), do: MapSet.new()

  defp flow_named_value_same_digest?(%{digest: digest}, digest) when is_binary(digest), do: true
  defp flow_named_value_same_digest?(_entry, _digest), do: false

  defp flow_named_value_next_version(%{version: version})
       when is_integer(version) and version > 0,
       do: version + 1

  defp flow_named_value_next_version(_entry), do: 1

  defp flow_named_value_version(version) when is_integer(version), do: version

  defp flow_named_value_version(version) when is_binary(version) do
    case Integer.parse(version) do
      {parsed, ""} -> parsed
      _ -> nil
    end
  end

  defp flow_named_value_version(_version), do: nil

  defp flow_named_value_digest_value(value) when is_binary(value) and value != "", do: value
  defp flow_named_value_digest_value(_value), do: nil

  defp flow_value_digest(value) do
    :crypto.hash(:sha256, Flow.encode_value(value))
    |> Base.encode16(case: :lower)
  end

  defp flow_create_idempotent_match?(state, existing, attrs) do
    id = Map.fetch!(attrs, :id)
    partition_key = Map.get(attrs, :partition_key)
    retention = flow_retention_for_create(state, attrs)

    comparable_attrs = %{
      id: id,
      type: Map.get(attrs, :type),
      state: Map.get(attrs, :state),
      partition_key: partition_key,
      payload_ref: flow_value_ref(attrs, :payload, id, 1, partition_key),
      parent_flow_id: Map.get(attrs, :parent_flow_id),
      root_flow_id: Map.get(attrs, :root_flow_id) || id,
      correlation_id: Map.get(attrs, :correlation_id),
      priority: Map.get(attrs, :priority, 0),
      ttl_ms: nil,
      retention_ttl_ms: Map.fetch!(retention, :ttl_ms),
      history_hot_max_events: Map.fetch!(retention, :history_hot_max_events),
      history_max_events: Map.fetch!(retention, :history_max_events)
    }

    Enum.all?(comparable_attrs, fn {key, value} -> Map.get(existing, key) == value end) and
      flow_create_idempotent_payload_match?(state, existing, attrs)
  end

  defp flow_create_idempotent_payload_match?(state, existing, %{payload: payload}) do
    case Map.get(existing, :payload_ref) do
      ref when is_binary(ref) and ref != "" ->
        case sm_store_batch_get(state, [ref], &sm_file_path/2) do
          [stored] when is_binary(stored) -> stored == Flow.encode_value(payload)
          _ -> false
        end

      _ ->
        false
    end
  end

  defp flow_create_idempotent_payload_match?(_state, _existing, _attrs), do: true

  defp flow_many_partitions_valid?(state, attrs_list) do
    with :ok <- flow_many_partition_keys_present?(attrs_list) do
      flow_many_same_state_machine_shard?(state, attrs_list)
    end
  end

  defp flow_many_partition_keys_present?(attrs_list) do
    if Enum.all?(attrs_list, fn attrs ->
         partition_key = Map.get(attrs, :partition_key)
         is_binary(partition_key) and partition_key != ""
       end) do
      :ok
    else
      {:error, "ERR flow partition_key is required"}
    end
  end

  defp flow_many_same_state_machine_shard?(
         %{instance_ctx: ctx, shard_index: shard_index},
         attrs_list
       )
       when is_map(ctx) do
    if Enum.all?(attrs_list, fn %{id: id, partition_key: partition_key} ->
         key = FlowKeys.state_key(id, partition_key)
         Router.shard_for(ctx, key) == shard_index
       end) do
      :ok
    else
      {:error, "ERR flow batch crosses shards"}
    end
  rescue
    _ -> :ok
  end

  defp flow_many_same_state_machine_shard?(_state, _attrs_list), do: :ok

  defp flow_create_many_unique?(attrs_list) do
    {_seen, result} =
      Enum.reduce_while(attrs_list, {MapSet.new(), :ok}, fn %{id: id}, {seen, :ok} ->
        if MapSet.member?(seen, id) do
          {:halt, {seen, {:error, "ERR flow duplicate id in batch"}}}
        else
          {:cont, {MapSet.put(seen, id), :ok}}
        end
      end)

    result
  end

  defp flow_create_many_prepare(state, attrs_list) do
    existing_records = flow_create_many_existing_states(state, attrs_list)

    attrs_list
    |> Enum.zip(existing_records)
    |> Enum.reduce_while({:ok, [], [], %{}}, fn
      {%{id: _id} = attrs, existing}, {:ok, acc, new_acc, retention_cache} ->
        case existing do
          nil ->
            {record, retention_cache} =
              flow_create_record_cached_retention(state, attrs, retention_cache)

            case flow_validate_record_keys(record) do
              :ok -> {:cont, {:ok, [record | acc], [{record, attrs} | new_acc], retention_cache}}
              {:error, _reason} = error -> {:halt, error}
            end

          :present ->
            {:halt, {:error, "ERR flow already exists"}}

          existing ->
            case flow_create_duplicate_result(state, existing, attrs) do
              {:ok, existing} -> {:cont, {:ok, [existing | acc], new_acc, retention_cache}}
              {:error, _reason} = error -> {:halt, error}
            end
        end
    end)
    |> case do
      {:ok, records, new_records, _retention_cache} ->
        {:ok, Enum.reverse(records), Enum.reverse(new_records)}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_create_many_existing_states(state, attrs_list) do
    keys = flow_state_keys_for_attrs(attrs_list)
    present = flow_state_keys_present(state, keys)

    attrs_list
    |> Enum.zip(Enum.zip(keys, present))
    |> Enum.map(fn
      {%{idempotent: true}, {key, true}} -> flow_read_record_by_key(state, key)
      {%{idempotent: true}, {_key, false}} -> nil
      {_attrs, {_key, true}} -> :present
      {_attrs, {_key, false}} -> nil
    end)
  end

  defp flow_create_many_apply(state, plans) do
    if flow_create_fast_staged_put_batch?(state) do
      Process.put(:sm_pending_fast_staged_put_batch, true)
    end

    records = Enum.map(plans, fn {record, _attrs} -> record end)

    with :ok <- flow_create_put_record_values(state, plans),
         :ok <- flow_create_put_state_records(state, records),
         :ok <- flow_due_put_many_new(state, records),
         :ok <- flow_index_put_many_new(state, records),
         :ok <- flow_create_put_history(state, records) do
      :ok
    end
  end

  defp flow_create_many_fast_apply(state, plans) do
    if flow_create_fast_staged_put_batch?(state) do
      Process.put(:sm_pending_fast_staged_put_batch, true)
    end

    with :ok <- flow_create_fast_put_record_values(state, plans),
         :ok <- flow_create_put_fast_state_records(state, plans),
         :ok <- flow_create_put_fast_due(state, plans),
         :ok <- flow_create_put_fast_indexes(state, plans),
         :ok <- flow_create_put_fast_history(state, plans) do
      :ok
    end
  end

  defp flow_create_fast_put_record_values(state, plans) do
    if flow_create_fast_plans_have_record_values?(plans) do
      record_attrs = Enum.map(plans, fn %{record: record, attrs: attrs} -> {record, attrs} end)
      flow_create_put_record_values(state, record_attrs)
    else
      :ok
    end
  end

  defp flow_create_fast_plans_have_record_values?(plans) do
    Enum.any?(plans, fn %{attrs: attrs} -> flow_attrs_have_record_values?(attrs) end)
  end

  defp flow_create_fast_staged_put_batch?(state) do
    not cross_shard_pending_active?() and not standalone_staged_apply?() and
      Map.get(state, :cross_shard_locks, %{}) == %{} and
      Process.get(:sm_pending_writes, []) == [] and Process.get(:sm_pending_values, %{}) == %{}
  end

  @flow_claim_due_phase_telemetry Application.compile_env(
                                    :ferricstore,
                                    :flow_claim_due_phase_telemetry,
                                    false
                                  )

  if @flow_claim_due_phase_telemetry do
    defmacrop flow_claim_due_phase(phase, metadata, fun) do
      quote do
        flow_claim_due_phase_emit(unquote(phase), unquote(metadata), unquote(fun))
      end
    end

    defmacrop flow_claim_due_internal_phase(phase, metadata, measurements, fun) do
      quote do
        flow_claim_due_internal_phase_emit(
          unquote(phase),
          unquote(metadata),
          unquote(measurements),
          unquote(fun)
        )
      end
    end
  else
    defmacrop flow_claim_due_phase(phase, metadata, fun) do
      quote generated: true do
        _ =
          case false do
            true -> {unquote(phase), unquote(metadata)}
            false -> :ok
          end

        unquote(fun).()
      end
    end

    defmacrop flow_claim_due_internal_phase(phase, metadata, measurements, fun) do
      quote generated: true do
        _ =
          case false do
            true -> {unquote(phase), unquote(metadata), unquote(measurements)}
            false -> :ok
          end

        unquote(fun).()
      end
    end
  end

  defp do_flow_claim_due(
         state,
         %{
           type: type,
           state: state_filter,
           worker: worker,
           lease_ms: lease_ms,
           limit: limit,
           priority: priority
         } = attrs
       ) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_keys) || Map.get(attrs, :partition_key)

    flow_claim_due_phase(
      :total,
      flow_claim_due_phase_meta(state, partition_key, priority, limit),
      fn ->
        state_filter =
          flow_claim_state_filter(state_filter, Map.get(attrs, :exclude_states, []))

        case flow_claim_due_priorities(
               state,
               type,
               state_filter,
               worker,
               lease_ms,
               now_ms,
               partition_key,
               flow_claim_priorities(priority),
               limit,
               []
             ) do
          {:error, _reason} = error -> error
          claimed -> {:ok, claimed}
        end
      end
    )
  end

  defp flow_claim_priorities(nil), do: [2, 1, 0]
  defp flow_claim_priorities(priority), do: [priority]

  @flow_due_any_index_enabled Application.compile_env(:ferricstore, :flow_due_any_index, false)
  defp flow_due_any_index_enabled?, do: @flow_due_any_index_enabled

  if @flow_claim_due_phase_telemetry do
    defp flow_claim_due_phase_emit(phase, metadata, fun) when is_function(fun, 0) do
      started_at = System.monotonic_time()
      result = fun.()

      measurements =
        result
        |> flow_claim_due_phase_measurements()
        |> Map.put(:duration_us, duration_us(started_at))

      :telemetry.execute(
        [:ferricstore, :flow, :claim_due_phase],
        measurements,
        Map.put(metadata, :phase, phase)
      )

      result
    end
  end

  if @flow_claim_due_phase_telemetry do
    defp flow_claim_due_phase_meta(state) do
      %{shard_index: Map.get(state, :shard_index)}
    end

    defp flow_claim_due_phase_meta(state, partition_key, priority, limit) do
      state
      |> flow_claim_due_phase_meta()
      |> Map.merge(%{
        partition_mode: flow_claim_due_partition_mode(partition_key),
        priority: priority,
        limit: limit
      })
    end
  else
    defp flow_claim_due_phase_meta(_state), do: %{}
    defp flow_claim_due_phase_meta(_state, _partition_key, _priority, _limit), do: %{}
  end

  if @flow_claim_due_phase_telemetry do
    defp flow_claim_due_partition_mode(:any), do: :any

    defp flow_claim_due_partition_mode(partition_keys) when is_list(partition_keys),
      do: :specific_many

    defp flow_claim_due_partition_mode(nil), do: :default
    defp flow_claim_due_partition_mode(_partition_key), do: :specific

    defp flow_claim_due_phase_measurements({:ok, records}) when is_list(records),
      do: %{items: length(records)}

    defp flow_claim_due_phase_measurements({plans, stale_due_ids, accepted})
         when is_list(plans) and is_list(stale_due_ids) and is_integer(accepted),
         do: %{plans: length(plans), stale_due_ids: length(stale_due_ids), accepted: accepted}

    defp flow_claim_due_phase_measurements({plans, stale_due_ids})
         when is_list(plans) and is_list(stale_due_ids),
         do: %{plans: length(plans), stale_due_ids: length(stale_due_ids)}

    defp flow_claim_due_phase_measurements({records, native_taken?})
         when is_list(records) and is_boolean(native_taken?),
         do: %{items: length(records)}

    defp flow_claim_due_phase_measurements(records) when is_list(records),
      do: %{items: length(records)}

    defp flow_claim_due_phase_measurements({:error, _reason}), do: %{errors: 1}
    defp flow_claim_due_phase_measurements(_result), do: %{}
  end

  if @flow_claim_due_phase_telemetry do
    defp flow_claim_due_internal_phase_emit(phase, metadata, measurements, fun)
         when is_function(fun, 0) do
      started_at = System.monotonic_time()
      result = fun.()

      :telemetry.execute(
        [:ferricstore, :flow, :claim_due_phase],
        Map.put(measurements, :duration_us, duration_us(started_at)),
        Map.put(metadata, :phase, phase)
      )

      result
    end
  end

  defp flow_claim_state_filter(state_filter, []), do: state_filter

  defp flow_claim_state_filter(state_filter, exclude_states),
    do: {:exclude, state_filter, exclude_states}

  defp flow_claim_due_priorities(
         _state,
         _type,
         _state_filter,
         _worker,
         _lease_ms,
         _now_ms,
         _partition_key,
         _priorities,
         limit,
         claimed
       )
       when length(claimed) >= limit do
    claimed |> Enum.reverse() |> Enum.take(limit)
  end

  defp flow_claim_due_priorities(
         _state,
         _type,
         _state_filter,
         _worker,
         _lease_ms,
         _now_ms,
         _partition_key,
         [],
         _limit,
         claimed
       ) do
    Enum.reverse(claimed)
  end

  defp flow_claim_due_priorities(
         state,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         [priority | rest],
         limit,
         claimed
       ) do
    due_keys =
      flow_claim_due_phase(
        :due_keys,
        flow_claim_due_phase_meta(state, partition_key, priority, limit),
        fn -> flow_claim_due_keys(state, type, state_filter, partition_key, priority) end
      )

    case flow_claim_due_scan_keys(
           state,
           due_keys,
           type,
           state_filter,
           worker,
           lease_ms,
           now_ms,
           partition_key,
           limit,
           claimed
         ) do
      {:error, _reason} = error ->
        error

      next_claimed ->
        flow_claim_due_priorities(
          state,
          type,
          state_filter,
          worker,
          lease_ms,
          now_ms,
          partition_key,
          rest,
          limit,
          next_claimed
        )
    end
  end

  defp flow_claim_due_scan_keys(
         state,
         [due_key],
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         limit,
         claimed
       ) do
    flow_ensure_due_index_ready(state, due_key)
    claimed_count = length(claimed)
    max_scan = max((limit - claimed_count) * 16, limit + 64)

    case flow_claim_due_scan(
           state,
           due_key,
           type,
           state_filter,
           worker,
           lease_ms,
           now_ms,
           partition_key,
           limit,
           max_scan,
           0,
           claimed_count,
           claimed
         ) do
      {:error, _reason} = error -> error
      {scanned_claimed, _scanned_count} -> scanned_claimed
    end
  end

  defp flow_claim_due_scan_keys(
         state,
         due_keys,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         limit,
         claimed
       ) do
    due_keys = Enum.sort(due_keys)
    Enum.each(due_keys, &flow_ensure_due_index_ready(state, &1))

    case flow_claim_due_scan_keys_native_multi(
           state,
           due_keys,
           type,
           state_filter,
           worker,
           lease_ms,
           now_ms,
           partition_key,
           limit,
           claimed
         ) do
      {:error, _reason} = error ->
        error

      {:ok, native_claimed} when length(native_claimed) >= limit ->
        native_claimed

      {:ok, native_claimed} ->
        native_claimed

      :fallback ->
        flow_claim_due_scan_key_rounds(
          state,
          due_keys,
          type,
          state_filter,
          worker,
          lease_ms,
          now_ms,
          partition_key,
          limit,
          claimed
        )
    end
  end

  defp flow_claim_due_scan_keys_native_multi(
         state,
         due_keys,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         limit,
         claimed
       ) do
    remaining = limit - length(claimed)

    cond do
      remaining <= 0 ->
        {:ok, claimed}

      flow_native_index(state) == nil ->
        :fallback

      true ->
        native = flow_native_index(state)
        batch_size = max(remaining, 32)
        max_scan = max(remaining * 16, remaining + 64)

        candidates =
          native
          |> NativeFlowIndex.claim_due_candidates(due_keys, now_ms, batch_size, max_scan)
          |> Enum.reduce(%{}, fn {due_key, id, score}, acc ->
            Map.update(acc, due_key, [{id, score}], &[{id, score} | &1])
          end)

        if map_size(candidates) == 0 do
          {:ok, claimed}
        else
          flow_claim_native_multi_candidate_groups(
            state,
            candidates,
            type,
            state_filter,
            worker,
            lease_ms,
            now_ms,
            partition_key,
            limit,
            claimed
          )
        end
    end
  end

  defp flow_claim_native_multi_candidate_groups(
         state,
         candidates,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         limit,
         claimed
       ) do
    claimed_count = length(claimed)

    candidates
    |> Enum.reduce_while({claimed_count, claimed}, fn {due_key, due_candidates},
                                                      {acc_count, acc} ->
      if acc_count >= limit do
        {:halt, {acc_count, acc}}
      else
        case flow_claim_candidate_batch(
               state,
               due_key,
               type,
               state_filter,
               worker,
               lease_ms,
               now_ms,
               partition_key,
               Enum.reverse(due_candidates),
               limit - acc_count,
               acc_count,
               acc
             ) do
          {:error, _reason} = error -> {:halt, error}
          {next_count, next_claimed} -> {:cont, {next_count, next_claimed}}
        end
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      {_count, next_claimed} -> {:ok, next_claimed}
    end
  end

  defp flow_claim_due_scan_key_rounds(
         _state,
         [],
         _type,
         _state_filter,
         _worker,
         _lease_ms,
         _now_ms,
         _partition_key,
         _limit,
         claimed
       ),
       do: claimed

  defp flow_claim_due_scan_key_rounds(
         state,
         due_keys,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         limit,
         claimed
       ) do
    flow_claim_due_scan_key_rounds(
      state,
      due_keys,
      type,
      state_filter,
      worker,
      lease_ms,
      now_ms,
      partition_key,
      limit,
      claimed,
      length(claimed)
    )
  end

  defp flow_claim_due_scan_key_rounds(
         _state,
         _due_keys,
         _type,
         _state_filter,
         _worker,
         _lease_ms,
         _now_ms,
         _partition_key,
         limit,
         claimed,
         claimed_count
       )
       when claimed_count >= limit,
       do: claimed

  defp flow_claim_due_scan_key_rounds(
         state,
         due_keys,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         limit,
         claimed,
         claimed_count
       ) do
    round_chunk =
      flow_claim_due_scan_key_round_chunk(limit - claimed_count, length(due_keys))

    scan_result =
      Enum.reduce_while(due_keys, {claimed, claimed_count, false}, fn due_key,
                                                                      {acc, acc_count,
                                                                       progressed?} ->
        if acc_count >= limit do
          {:halt, {acc, acc_count, progressed?}}
        else
          target_count = min(limit, acc_count + round_chunk)

          case flow_claim_due_scan(
                 state,
                 due_key,
                 type,
                 state_filter,
                 worker,
                 lease_ms,
                 now_ms,
                 partition_key,
                 target_count,
                 max((target_count - acc_count) * 16, 32),
                 0,
                 acc_count,
                 acc
               ) do
            {:error, _reason} = error ->
              {:halt, error}

            {next_acc, next_count} ->
              {:cont, {next_acc, next_count, progressed? or next_count > acc_count}}
          end
        end
      end)

    case scan_result do
      {:error, _reason} = error ->
        error

      {next_claimed, next_count, progressed?} ->
        cond do
          next_count >= limit ->
            next_claimed

          progressed? ->
            flow_claim_due_scan_key_rounds(
              state,
              due_keys,
              type,
              state_filter,
              worker,
              lease_ms,
              now_ms,
              partition_key,
              limit,
              next_claimed,
              next_count
            )

          true ->
            next_claimed
        end
    end
  end

  defp flow_claim_due_scan_key_round_chunk(remaining, due_key_count)
       when remaining > 0 and due_key_count > 0 do
    max(1, div(remaining + due_key_count - 1, due_key_count))
  end

  defp flow_claim_due_scan_key_round_chunk(_remaining, _due_key_count), do: 1

  defp flow_claim_due_keys(state, type, :any, partition_keys, priority)
       when is_list(partition_keys) do
    if flow_due_any_index_enabled?() do
      Enum.map(partition_keys, &FlowKeys.due_any_key(type, priority, &1))
    else
      Enum.flat_map(partition_keys, fn partition_key ->
        flow_claim_due_matching_keys(state, type, :any, partition_key, priority)
      end)
    end
  end

  defp flow_claim_due_keys(
         state,
         type,
         {:exclude, :any, _exclude_states} = state_filter,
         partition_keys,
         priority
       )
       when is_list(partition_keys) do
    if flow_due_any_index_enabled?() do
      Enum.map(partition_keys, &FlowKeys.due_any_key(type, priority, &1))
    else
      Enum.flat_map(partition_keys, fn partition_key ->
        flow_claim_due_matching_keys(state, type, state_filter, partition_key, priority)
      end)
    end
  end

  defp flow_claim_due_keys(_state, type, state_filter, partition_keys, priority)
       when is_binary(state_filter) and is_list(partition_keys) do
    Enum.map(partition_keys, &FlowKeys.due_key(type, state_filter, priority, &1))
  end

  defp flow_claim_due_keys(_state, type, states, partition_keys, priority)
       when is_list(states) and is_list(partition_keys) do
    for partition_key <- partition_keys, state <- states do
      FlowKeys.due_key(type, state, priority, partition_key)
    end
  end

  defp flow_claim_due_keys(_state, type, state_filter, partition_key, priority)
       when partition_key not in [:any, :auto] and is_binary(state_filter) do
    [FlowKeys.due_key(type, state_filter, priority, partition_key)]
  end

  defp flow_claim_due_keys(_state, type, states, partition_key, priority)
       when partition_key not in [:any, :auto] and is_list(states) do
    Enum.map(states, &FlowKeys.due_key(type, &1, priority, partition_key))
  end

  defp flow_claim_due_keys(state, type, :any, partition_key, priority)
       when partition_key not in [:any, :auto] do
    if flow_due_any_index_enabled?() do
      [FlowKeys.due_any_key(type, priority, partition_key)]
    else
      flow_claim_due_matching_keys(state, type, :any, partition_key, priority)
    end
  end

  defp flow_claim_due_keys(
         state,
         type,
         {:exclude, :any, _exclude_states} = state_filter,
         partition_key,
         priority
       )
       when partition_key not in [:any, :auto] do
    if flow_due_any_index_enabled?() do
      [FlowKeys.due_any_key(type, priority, partition_key)]
    else
      flow_claim_due_matching_keys(state, type, state_filter, partition_key, priority)
    end
  end

  defp flow_claim_due_keys(state, type, state_filter, partition_key, priority) do
    flow_claim_due_matching_keys(state, type, state_filter, partition_key, priority)
  end

  defp flow_claim_due_matching_keys(state, type, state_filter, partition_key, priority) do
    state
    |> flow_claim_index_count_keys()
    |> Enum.filter(&flow_due_key_matches?(&1, type, state_filter, partition_key, priority))
  end

  defp flow_due_key_matches?(key, type, state_filter, partition_key, priority)
       when is_binary(key) do
    String.starts_with?(key, "f:{f") and
      flow_due_key_partition_match?(key, partition_key) and
      flow_due_key_state_match?(key, type, state_filter) and
      String.ends_with?(key, ":p" <> Integer.to_string(priority))
  end

  defp flow_due_key_matches?(_key, _type, _state_filter, _partition_key, _priority), do: false

  defp flow_due_key_partition_match?(_key, :any), do: true

  defp flow_due_key_partition_match?(key, :auto) do
    String.starts_with?(key, "f:{fa:") and
      (String.contains?(key, "}:d:") or String.contains?(key, "}:da:"))
  end

  defp flow_due_key_partition_match?(key, partition_keys) when is_list(partition_keys) do
    Enum.any?(partition_keys, &flow_due_key_partition_match?(key, &1))
  end

  defp flow_due_key_partition_match?(key, partition_key) do
    tag = FlowKeys.tag(partition_key)

    String.starts_with?(key, "f:" <> tag <> ":d:") or
      String.starts_with?(key, "f:" <> tag <> ":da:")
  end

  defp flow_due_key_state_match?(key, type, :any) do
    if flow_due_any_index_enabled?() do
      String.contains?(key, "}:da:" <> type <> ":p")
    else
      String.contains?(key, "}:d:" <> type <> ":")
    end
  end

  defp flow_due_key_state_match?(key, type, {:exclude, state_filter, _exclude_states}) do
    flow_due_key_state_match?(key, type, state_filter)
  end

  defp flow_due_key_state_match?(key, type, states) when is_list(states) do
    Enum.any?(states, &flow_due_key_state_match?(key, type, &1))
  end

  defp flow_due_key_state_match?(key, type, state) when is_binary(state) do
    String.contains?(key, "}:d:" <> type <> ":" <> state <> ":p")
  end

  defp flow_due_key_state_match?(_key, _type, _state), do: false

  defp flow_claim_due_scan(
         _state,
         _due_key,
         _type,
         _expected_state,
         _worker,
         _lease_ms,
         _now_ms,
         _partition_key,
         limit,
         _max_scan,
         _scanned,
         claimed_count,
         claimed
       )
       when claimed_count >= limit do
    {claimed, claimed_count}
  end

  defp flow_claim_due_scan(
         _state,
         _due_key,
         _type,
         _expected_state,
         _worker,
         _lease_ms,
         _now_ms,
         _partition_key,
         _limit,
         max_scan,
         scanned,
         claimed_count,
         claimed
       )
       when scanned >= max_scan do
    {claimed, claimed_count}
  end

  defp flow_claim_due_scan(
         state,
         due_key,
         type,
         expected_state,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         limit,
         max_scan,
         scanned,
         claimed_count,
         claimed
       ) do
    remaining = limit - claimed_count
    batch_size = min(max(remaining, 32), max_scan - scanned)

    candidates =
      flow_claim_due_phase(
        :range_slice,
        Map.put(
          flow_claim_due_phase_meta(state, partition_key, nil, remaining),
          :batch_size,
          batch_size
        ),
        fn ->
          flow_claim_due_candidate_slice(state, due_key, now_ms, batch_size)
        end
      )

    if candidates == [] do
      {claimed, claimed_count}
    else
      case flow_claim_candidate_batch(
             state,
             due_key,
             type,
             expected_state,
             worker,
             lease_ms,
             now_ms,
             partition_key,
             candidates,
             limit - claimed_count,
             claimed_count,
             claimed
           ) do
        {:error, _reason} = error ->
          error

        {next_claimed_count, next_claimed} ->
          flow_claim_due_scan(
            state,
            due_key,
            type,
            expected_state,
            worker,
            lease_ms,
            now_ms,
            partition_key,
            limit,
            max_scan,
            scanned + length(candidates),
            next_claimed_count,
            next_claimed
          )
      end
    end
  end

  defp flow_claim_due_candidate_slice(state, due_key, now_ms, batch_size) do
    case flow_native_index(state) do
      nil ->
        flow_claim_due_ets_candidate_slice(state, due_key, now_ms, batch_size)

      native ->
        candidates =
          native
          |> NativeFlowIndex.claim_due_candidates([due_key], now_ms, batch_size, batch_size)
          |> Enum.map(fn
            {^due_key, id, score} -> {id, score}
            {_key, id, score} -> {id, score}
          end)

        candidates
    end
  end

  defp flow_claim_due_ets_candidate_slice(state, due_key, now_ms, batch_size) do
    if flow_index_tables?(state) do
      FlowIndex.range_slice(
        state.flow_index_name,
        due_key,
        :neg_inf,
        {:inclusive, now_ms * 1.0},
        false,
        0,
        batch_size
      )
    else
      ZSetIndex.range_slice(
        state.zset_score_index_name,
        due_key,
        :neg_inf,
        {:inclusive, now_ms * 1.0},
        false,
        0,
        batch_size
      )
    end
  end

  defp flow_claim_candidate_batch(
         state,
         due_key,
         type,
         expected_state,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         candidates,
         remaining,
         claimed_count,
         claimed
       ) do
    {plans, stale_due_ids} =
      flow_plan_claim_candidates(
        state,
        due_key,
        type,
        expected_state,
        worker,
        lease_ms,
        now_ms,
        partition_key,
        candidates,
        remaining
      )

    phase_meta =
      state
      |> flow_claim_due_phase_meta(partition_key, nil, remaining)
      |> Map.merge(%{candidates: length(candidates)})

    case flow_apply_claim_batch(state, due_key, plans, stale_due_ids, now_ms) do
      :ok ->
        next_claimed =
          flow_claim_due_phase(:return_assemble, phase_meta, fn ->
            Enum.reduce(plans, claimed, fn plan, acc ->
              {_record, next} = flow_claim_plan_pair(plan)
              [next | acc]
            end)
          end)

        {claimed_count + length(plans), next_claimed}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_plan_claim_candidates(
         state,
         due_key,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         candidates,
         remaining
       ) do
    case flow_plan_claim_candidates_native(
           state,
           due_key,
           type,
           state_filter,
           worker,
           lease_ms,
           now_ms,
           partition_key,
           candidates,
           remaining
         ) do
      {:ok, result} ->
        result

      :fallback ->
        flow_plan_claim_candidates_elixir(
          state,
          due_key,
          type,
          state_filter,
          worker,
          lease_ms,
          now_ms,
          partition_key,
          candidates,
          remaining
        )
    end
  end

  defp flow_plan_claim_candidates_elixir(
         state,
         due_key,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         candidates,
         remaining
       ) do
    phase_meta =
      state
      |> flow_claim_due_phase_meta(partition_key, nil, remaining)
      |> Map.merge(%{candidates: length(candidates)})

    records =
      flow_claim_due_phase(:hydrate_records, phase_meta, fn ->
        flow_read_claim_candidate_records(state, partition_key, due_key, candidates)
      end)

    {plans, stale_due_ids, _count} =
      flow_claim_due_phase(:plan_candidates, phase_meta, fn ->
        flow_plan_claim_candidate_records(
          candidates,
          records,
          type,
          state_filter,
          worker,
          lease_ms,
          now_ms,
          remaining
        )
      end)

    {Enum.reverse(plans), Enum.reverse(stale_due_ids)}
  end

  defp flow_plan_claim_candidate_records(
         candidates,
         records,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         remaining
       ) do
    flow_plan_claim_candidate_records(
      candidates,
      records,
      type,
      state_filter,
      worker,
      lease_ms,
      now_ms,
      remaining,
      [],
      [],
      0
    )
  end

  defp flow_plan_claim_candidate_records(
         _candidates,
         _records,
         _type,
         _state_filter,
         _worker,
         _lease_ms,
         _now_ms,
         remaining,
         plans,
         stale_due_ids,
         count
       )
       when count >= remaining do
    {plans, stale_due_ids, count}
  end

  defp flow_plan_claim_candidate_records(
         [],
         _records,
         _type,
         _state_filter,
         _worker,
         _lease_ms,
         _now_ms,
         _remaining,
         plans,
         stale_due_ids,
         count
       ) do
    {plans, stale_due_ids, count}
  end

  defp flow_plan_claim_candidate_records(
         _candidates,
         [],
         _type,
         _state_filter,
         _worker,
         _lease_ms,
         _now_ms,
         _remaining,
         plans,
         stale_due_ids,
         count
       ) do
    {plans, stale_due_ids, count}
  end

  defp flow_plan_claim_candidate_records(
         [{id, due_score} | candidates],
         [record | records],
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         remaining,
         plans,
         stale_due_ids,
         count
       ) do
    case flow_prepare_claim_candidate_record(
           record,
           id,
           type,
           state_filter,
           worker,
           lease_ms,
           now_ms,
           due_score
         ) do
      {:ok, record, next, from_due_score} ->
        flow_plan_claim_candidate_records(
          candidates,
          records,
          type,
          state_filter,
          worker,
          lease_ms,
          now_ms,
          remaining,
          [{record, next, from_due_score} | plans],
          stale_due_ids,
          count + 1
        )

      :delete_due ->
        flow_plan_claim_candidate_records(
          candidates,
          records,
          type,
          state_filter,
          worker,
          lease_ms,
          now_ms,
          remaining,
          plans,
          [id | stale_due_ids],
          count
        )

      {:skip, _restore_score} ->
        flow_plan_claim_candidate_records(
          candidates,
          records,
          type,
          state_filter,
          worker,
          lease_ms,
          now_ms,
          remaining,
          plans,
          stale_due_ids,
          count
        )
    end
  end

  defp flow_plan_claim_candidates_native(
         state,
         due_key,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         partition_key,
         candidates,
         remaining
       )
       when is_binary(type) and is_binary(worker) do
    cond do
      remaining <= 0 ->
        {:ok, {[], []}}

      not flow_index_tables?(state) ->
        :fallback

      flow_native_index(state) == nil ->
        :fallback

      true ->
        with {:ok,
              {expected_state, from_due_key, to_due_key, from_state_key, to_state_key,
               inflight_key, worker_key, state_key_prefix}} <-
               flow_native_claim_keys(due_key, type, state_filter, worker) do
          phase_meta =
            state
            |> flow_claim_due_phase_meta(partition_key, nil, remaining)
            |> Map.merge(%{candidates: length(candidates)})

          values =
            flow_claim_due_phase(:hydrate_values, phase_meta, fn ->
              flow_read_claim_candidate_values(state, partition_key, due_key, candidates)
            end)

          if flow_lmdb_mirror?(state) and Enum.any?(values, &is_nil/1) do
            :fallback
          else
            native_result =
              flow_claim_due_phase(:plan_candidates_native, phase_meta, fn ->
                Ferricstore.Flow.NativeOrderedIndex.plan_claims(
                  candidates,
                  values,
                  type,
                  expected_state,
                  worker,
                  lease_ms,
                  now_ms,
                  remaining,
                  from_due_key,
                  to_due_key,
                  from_state_key,
                  to_state_key,
                  inflight_key,
                  worker_key,
                  state_key_prefix
                )
              end)

            case native_result do
              {:ok, native_plans, stale_due_ids, count} ->
                case flow_decode_native_claim_plans(native_plans) do
                  {:ok, plans} when length(plans) == count -> {:ok, {plans, stale_due_ids}}
                  :fallback -> :fallback
                  _ -> :fallback
                end

              :fallback ->
                :fallback
            end
          end
        else
          _ -> :fallback
        end
    end
  end

  defp flow_plan_claim_candidates_native(
         _state,
         _due_key,
         _type,
         _state_filter,
         _worker,
         _lease_ms,
         _now_ms,
         _partition_key,
         _candidates,
         _remaining
       ),
       do: :fallback

  defp flow_native_claim_keys(due_key, type, state_filter, worker) do
    with {:ok, tag} <- flow_due_key_tag(due_key),
         {:ok, expected_state} <- flow_due_key_expected_state(state_filter),
         {:ok, priority} <- flow_due_key_priority(due_key, tag, type, expected_state),
         false <- expected_state == "running" do
      from_due_key = due_key
      to_due_key = "f:" <> tag <> ":d:" <> type <> ":running:p" <> Integer.to_string(priority)
      from_state_key = "f:" <> tag <> ":i:s:" <> type <> ":" <> expected_state
      to_state_key = "f:" <> tag <> ":i:s:" <> type <> ":running"
      inflight_key = "f:" <> tag <> ":i:r:" <> type
      worker_key = "f:" <> tag <> ":i:w:" <> worker
      state_key_prefix = "f:" <> tag <> ":s:"

      with :ok <- flow_validate_key_size(from_due_key),
           :ok <- flow_validate_key_size(to_due_key),
           :ok <- flow_validate_key_size(from_state_key),
           :ok <- flow_validate_key_size(to_state_key),
           :ok <- flow_validate_key_size(inflight_key),
           :ok <- flow_validate_key_size(worker_key) do
        {:ok,
         {expected_state, from_due_key, to_due_key, from_state_key, to_state_key, inflight_key,
          worker_key, state_key_prefix}}
      end
    end
  end

  defp flow_due_key_expected_state({:exclude, state_filter, exclude_states})
       when is_list(exclude_states) do
    with {:ok, expected_state} <- flow_due_key_expected_state(state_filter),
         false <- expected_state in exclude_states do
      {:ok, expected_state}
    else
      _ -> :error
    end
  end

  defp flow_due_key_expected_state(state_filter) when is_binary(state_filter),
    do: {:ok, state_filter}

  defp flow_due_key_expected_state(_state_filter), do: :error

  defp flow_due_key_tag(due_key) when is_binary(due_key) do
    case flow_due_key_tag_match(due_key) do
      {pos, _len} when pos >= 2 ->
        {:ok, binary_part(due_key, 2, pos + 1 - 2)}

      nil ->
        :error
    end
  end

  defp flow_due_key_tag(_due_key), do: :error

  defp flow_due_key_tag_match(due_key) do
    case :binary.match(due_key, "}:d:") do
      {pos, len} ->
        {pos, len}

      :nomatch ->
        case :binary.match(due_key, "}:da:") do
          {pos, len} -> {pos, len}
          :nomatch -> nil
        end
    end
  end

  defp flow_due_key_priority(due_key, tag, type, expected_state)
       when is_binary(due_key) and is_binary(tag) and is_binary(type) and
              is_binary(expected_state) do
    prefix = "f:" <> tag <> ":d:" <> type <> ":" <> expected_state <> ":p"
    prefix_size = byte_size(prefix)

    with true <- byte_size(due_key) > prefix_size,
         <<^prefix::binary-size(prefix_size), priority_bin::binary>> <- due_key do
      case Integer.parse(priority_bin) do
        {priority, ""} -> {:ok, priority}
        _ -> :error
      end
    else
      _ -> :error
    end
  end

  defp flow_due_key_priority(_due_key, _tag, _type, _expected_state), do: :error

  defp flow_decode_native_claim_plans(native_plans) do
    native_plans
    |> Enum.reduce_while({:ok, []}, fn
      {next_value, entry, state_key, previous_history_ms}, {:ok, acc}
      when is_binary(next_value) and is_tuple(entry) and is_binary(state_key) ->
        with next when is_map(next) <- flow_decode_native_claim_record(next_value) do
          {:cont,
           {:ok,
            [
              {:native_claim, next, entry, state_key, next_value, previous_history_ms}
              | acc
            ]}}
        else
          _ -> {:halt, :fallback}
        end

      _other, _acc ->
        {:halt, :fallback}
    end)
    |> case do
      {:ok, plans} -> {:ok, Enum.reverse(plans)}
      :fallback -> :fallback
    end
  end

  defp flow_decode_native_claim_record(value) do
    Flow.decode_record(value)
  rescue
    _ -> nil
  end

  defp flow_read_claim_candidate_records(state, :any, due_key, candidates) do
    prefix = flow_state_key_prefix_from_due_key(due_key)

    if flow_lmdb_mirror?(state) do
      keys =
        if is_binary(prefix) do
          Enum.map(candidates, fn {id, _score} -> prefix <> id end)
        else
          Enum.map(candidates, fn {id, _score} -> FlowKeys.state_key(id, nil) end)
        end

      flow_read_records_by_keys(state, keys)
    else
      flow_read_claim_candidate_hot_records(state, candidates, prefix, nil)
    end
  end

  defp flow_read_claim_candidate_records(state, partition_key, due_key, candidates) do
    prefix = flow_state_key_prefix_from_due_key(due_key)

    if flow_lmdb_mirror?(state) do
      keys =
        if is_binary(prefix) do
          Enum.map(candidates, fn {id, _score} -> prefix <> id end)
        else
          Enum.map(candidates, fn {id, _score} -> FlowKeys.state_key(id, partition_key) end)
        end

      flow_read_records_by_keys(state, keys)
    else
      flow_read_claim_candidate_hot_records(state, candidates, prefix, partition_key)
    end
  end

  defp flow_read_claim_candidate_values(state, :any, due_key, candidates) do
    prefix = flow_state_key_prefix_from_due_key(due_key)
    flow_read_claim_candidate_hot_values(state, candidates, prefix, nil)
  end

  defp flow_read_claim_candidate_values(state, partition_key, due_key, candidates) do
    prefix = flow_state_key_prefix_from_due_key(due_key)
    flow_read_claim_candidate_hot_values(state, candidates, prefix, partition_key)
  end

  defp flow_read_claim_candidate_hot_values(state, candidates, prefix, partition_key) do
    candidates
    |> flow_read_claim_candidate_hot_values_loop(state, prefix, partition_key, [])
    |> Enum.reverse()
  end

  defp flow_read_claim_candidate_hot_values_loop([], _state, _prefix, _partition_key, acc),
    do: acc

  defp flow_read_claim_candidate_hot_values_loop(
         [{id, _score} | rest],
         state,
         prefix,
         partition_key,
         acc
       ) do
    key =
      if is_binary(prefix) do
        prefix <> id
      else
        FlowKeys.state_key(id, partition_key)
      end

    flow_read_claim_candidate_hot_values_loop(
      rest,
      state,
      prefix,
      partition_key,
      [flow_read_hot_state_value(state, key) | acc]
    )
  end

  defp flow_read_hot_state_value(state, key) do
    case :ets.lookup(state.ets, key) do
      [{^key, value, 0, _lfu, _pending, _touched_at, _disk_size}] when is_binary(value) ->
        value

      [{^key, value, expire_at_ms, _lfu, _pending, _touched_at, _disk_size}]
      when is_binary(value) and is_integer(expire_at_ms) ->
        if expire_at_ms > apply_now_ms(), do: value, else: nil

      _other ->
        case ets_lookup(state, key) do
          {:ok, value} when is_binary(value) -> value
          _ -> nil
        end
    end
  end

  defp flow_read_claim_candidate_hot_records(state, candidates, prefix, partition_key) do
    candidates
    |> flow_read_claim_candidate_hot_records_loop(state, prefix, partition_key, [])
    |> Enum.reverse()
  end

  defp flow_read_claim_candidate_hot_records_loop([], _state, _prefix, _partition_key, acc),
    do: acc

  defp flow_read_claim_candidate_hot_records_loop(
         [{id, _score} | rest],
         state,
         prefix,
         partition_key,
         acc
       ) do
    key =
      if is_binary(prefix) do
        prefix <> id
      else
        FlowKeys.state_key(id, partition_key)
      end

    flow_read_claim_candidate_hot_records_loop(
      rest,
      state,
      prefix,
      partition_key,
      [flow_read_hot_state_record(state, key) | acc]
    )
  end

  defp flow_state_key_prefix_from_due_key(due_key) when is_binary(due_key) do
    case flow_due_key_tag_match(due_key) do
      {pos, _len} when pos >= 2 ->
        tag = binary_part(due_key, 2, pos + 1 - 2)
        "f:" <> tag <> ":s:"

      nil ->
        nil
    end
  end

  defp do_flow_extend_lease(state, %{id: id, lease_token: lease_token} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    lease_ms = Map.fetch!(attrs, :lease_ms)
    partition_key = Map.get(attrs, :partition_key)

    with {:ok, record} <- flow_require_record(state, id, partition_key),
         :ok <- flow_require_running_lease(record, lease_token),
         :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)) do
      version = Map.fetch!(record, :version) + 1
      deadline_ms = now_ms + lease_ms

      next =
        record
        |> Map.merge(%{
          version: version,
          updated_at_ms: now_ms,
          ttl_ms: nil,
          retention_ttl_ms: Map.get(record, :retention_ttl_ms),
          history_hot_max_events: Map.get(record, :history_hot_max_events),
          history_max_events: Map.get(record, :history_max_events),
          lease_deadline_ms: deadline_ms,
          next_run_at_ms: deadline_ms
        })

      with :ok <- flow_validate_record_keys(record),
           :ok <- flow_validate_record_keys(next),
           :ok <- flow_transition_move_indexes(state, [{record, next}]),
           :ok <-
             flow_put_state_record(
               state,
               FlowKeys.state_key(next.id, Map.get(next, :partition_key)),
               next
             ),
           :ok <- flow_history_put_planned(state, record, next, "lease_extended", now_ms),
           :ok <- flow_after_history_put(state, next) do
        {:ok, next}
      end
    end
  end

  defp do_flow_complete(state, %{id: id, lease_token: lease_token} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)

    with {:ok, record} <- flow_require_record(state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_complete_existing_record(record, attrs, lease_token, now_ms),
         :ok <- flow_apply_complete(state, record, next, partition_key, now_ms, attrs) do
      :ok
    end
  end

  defp do_flow_complete_many(state, %{records: [_ | _] = attrs_list}) do
    with :ok <- flow_many_partitions_valid?(state, attrs_list),
         :ok <- flow_transition_many_unique?(attrs_list),
         {:ok, plans, has_record_values?, has_after_terminal?} <-
           flow_complete_many_prepare(state, attrs_list),
         :ok <- flow_complete_many_apply(state, plans, has_record_values?, has_after_terminal?) do
      :ok
    end
  end

  defp do_flow_complete_many(_state, _attrs),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp flow_prepare_complete_existing_record(record, attrs, lease_token, now_ms) do
    with :ok <- flow_require_running_lease(record, lease_token),
         :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)) do
      version = Map.fetch!(record, :version) + 1
      id = Map.fetch!(record, :id)
      partition_key = Map.get(record, :partition_key)

      payload_ref =
        flow_value_ref(
          attrs,
          :payload,
          id,
          version,
          partition_key,
          Map.get(record, :payload_ref)
        )

      result_ref = flow_value_ref(attrs, :result, id, version, partition_key)
      retention_ttl_ms = Map.get(attrs, :ttl_ms) || Map.get(record, :retention_ttl_ms)

      with {:ok, value_refs} <- flow_named_value_refs(record, attrs, id, version, partition_key) do
        next =
          %{
            record
            | state: "completed",
              version: version,
              updated_at_ms: now_ms,
              payload_ref: payload_ref,
              result_ref: result_ref,
              ttl_ms: nil,
              retention_ttl_ms: retention_ttl_ms,
              lease_owner: nil,
              lease_token: nil,
              lease_deadline_ms: 0,
              next_run_at_ms: nil
          }
          |> flow_put_record_value_refs(value_refs)
          |> flow_stamp_terminal_retention(now_ms)

        with :ok <- flow_validate_terminal_state_index_key(next) do
          {:ok, record, next}
        end
      end
    end
  end

  defp flow_apply_complete(state, record, next, partition_key, now_ms, attrs) do
    plans = [{record, next}]

    with :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_transition_move_indexes(state, plans),
         :ok <-
           flow_put_state_record(
             state,
             FlowKeys.state_key(next.id, partition_key),
             next
           ),
         :ok <- flow_history_put_planned(state, record, next, "completed", now_ms),
         :ok <- flow_after_history_put(state, next),
         :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms),
         :ok <- flow_maybe_apply_child_terminal(state, next, "completed", now_ms) do
      :ok
    end
  end

  defp flow_complete_many_prepare(state, attrs_list) do
    existing_records = flow_read_records(state, attrs_list)

    attrs_list
    |> Enum.zip(existing_records)
    |> Enum.reduce_while({:ok, [], false, false}, fn
      {%{id: _id, lease_token: lease_token} = attrs, existing},
      {:ok, acc, has_values?, has_after_terminal?} ->
        now_ms = flow_attrs_now_ms(attrs)

        case existing do
          nil ->
            {:halt, {:error, "ERR flow not found"}}

          record ->
            case flow_prepare_complete_existing_record(record, attrs, lease_token, now_ms) do
              {:ok, record, next} ->
                {:cont,
                 {:ok, [{record, next, attrs} | acc],
                  has_values? or flow_attrs_have_record_values?(attrs),
                  has_after_terminal? or flow_terminal_after_required?(:complete, next)}}

              {:error, _reason} = error ->
                {:halt, error}
            end
        end

      {_bad, _existing}, {:ok, _acc, _has_values?, _has_after_terminal?} ->
        {:halt, {:error, "ERR flow id must be a non-empty string"}}
    end)
    |> case do
      {:ok, plans, has_record_values?, has_after_terminal?} ->
        {:ok, Enum.reverse(plans), has_record_values?, has_after_terminal?}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_complete_many_apply(state, plans, has_record_values?, has_after_terminal?) do
    with :ok <- flow_many_put_record_values(state, plans, has_record_values?),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <- flow_claim_put_state_records(state, plans),
         :ok <- flow_many_put_history(state, plans, "completed"),
         :ok <- flow_many_after_terminal(state, plans, "completed", has_after_terminal?) do
      :ok
    end
  end

  defp do_flow_terminal_pipeline_batch(state, op, %{records: [_ | _] = attrs_list})
       when op in [:complete, :retry, :fail, :cancel] do
    {results, plans, has_record_values?, has_after_terminal?} =
      flow_terminal_pipeline_prepare(state, op, attrs_list)

    case flow_terminal_pipeline_apply(
           state,
           op,
           Enum.reverse(plans),
           has_record_values?,
           has_after_terminal?
         ) do
      :ok -> Enum.reverse(results)
      {:error, _reason} = error -> error
    end
  end

  defp do_flow_terminal_pipeline_batch(_state, _op, _attrs),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp flow_terminal_pipeline_prepare(state, op, attrs_list) do
    case flow_terminal_pipeline_unique_records(state, attrs_list) do
      {:ok, records} ->
        flow_terminal_pipeline_prepare_records(
          state,
          op,
          attrs_list,
          records,
          [],
          [],
          false,
          false
        )

      :fallback ->
        flow_terminal_pipeline_prepare(state, op, attrs_list, %{}, [], [], false, false)
    end
  end

  defp flow_terminal_pipeline_unique_records(state, attrs_list) do
    attrs_list
    |> Enum.reduce_while({:ok, [], MapSet.new()}, fn
      %{id: id} = attrs, {:ok, keys, seen} when is_binary(id) and id != "" ->
        key = FlowKeys.state_key(id, Map.get(attrs, :partition_key))

        if MapSet.member?(seen, key) do
          {:halt, :fallback}
        else
          {:cont, {:ok, [key | keys], MapSet.put(seen, key)}}
        end

      _attrs, _acc ->
        {:halt, :fallback}
    end)
    |> case do
      {:ok, keys, _seen} -> {:ok, flow_read_records_by_keys(state, Enum.reverse(keys))}
      :fallback -> :fallback
    end
  end

  defp flow_terminal_pipeline_prepare_records(
         _state,
         _op,
         [],
         [],
         results,
         plans,
         has_record_values?,
         has_after_terminal?
       ) do
    {results, plans, has_record_values?, has_after_terminal?}
  end

  defp flow_terminal_pipeline_prepare_records(
         state,
         op,
         [%{id: id} = attrs | rest_attrs],
         [record | rest_records],
         results,
         plans,
         has_record_values?,
         has_after_terminal?
       )
       when is_binary(id) and id != "" do
    now_ms = flow_attrs_now_ms(attrs)

    case flow_terminal_pipeline_prepare_one(state, op, record, attrs, now_ms) do
      {:ok, next, plan} ->
        flow_terminal_pipeline_prepare_records(
          state,
          op,
          rest_attrs,
          rest_records,
          [:ok | results],
          [plan | plans],
          has_record_values? or flow_attrs_have_record_values?(attrs),
          has_after_terminal? or flow_terminal_after_required?(op, next)
        )

      {:error, _reason} = error ->
        flow_terminal_pipeline_prepare_records(
          state,
          op,
          rest_attrs,
          rest_records,
          [error | results],
          plans,
          has_record_values?,
          has_after_terminal?
        )
    end
  end

  defp flow_terminal_pipeline_prepare_records(
         state,
         op,
         [_bad | rest_attrs],
         [_record | rest_records],
         results,
         plans,
         has_record_values?,
         has_after_terminal?
       ) do
    flow_terminal_pipeline_prepare_records(
      state,
      op,
      rest_attrs,
      rest_records,
      [{:error, "ERR flow id must be a non-empty string"} | results],
      plans,
      has_record_values?,
      has_after_terminal?
    )
  end

  defp flow_terminal_pipeline_prepare_records(
         state,
         op,
         _attrs,
         _records,
         results,
         plans,
         has_record_values?,
         has_after_terminal?
       ) do
    flow_terminal_pipeline_prepare(
      state,
      op,
      [],
      %{},
      results,
      plans,
      has_record_values?,
      has_after_terminal?
    )
  end

  defp flow_terminal_pipeline_prepare(
         _state,
         _op,
         [],
         _virtual_records,
         results,
         plans,
         has_record_values?,
         has_after_terminal?
       ) do
    {results, plans, has_record_values?, has_after_terminal?}
  end

  defp flow_terminal_pipeline_prepare(
         state,
         op,
         [%{id: id} = attrs | rest],
         virtual_records,
         results,
         plans,
         has_record_values?,
         has_after_terminal?
       )
       when is_binary(id) and id != "" do
    partition_key = Map.get(attrs, :partition_key)
    state_key = FlowKeys.state_key(id, partition_key)
    now_ms = flow_attrs_now_ms(attrs)
    record = Map.get(virtual_records, state_key) || flow_read_record(state, id, partition_key)

    case flow_terminal_pipeline_prepare_one(state, op, record, attrs, now_ms) do
      {:ok, next, plan} ->
        flow_terminal_pipeline_prepare(
          state,
          op,
          rest,
          Map.put(virtual_records, state_key, next),
          [:ok | results],
          [plan | plans],
          has_record_values? or flow_attrs_have_record_values?(attrs),
          has_after_terminal? or flow_terminal_after_required?(op, next)
        )

      {:error, _reason} = error ->
        flow_terminal_pipeline_prepare(
          state,
          op,
          rest,
          virtual_records,
          [error | results],
          plans,
          has_record_values?,
          has_after_terminal?
        )
    end
  end

  defp flow_terminal_pipeline_prepare(
         state,
         op,
         [_bad | rest],
         virtual_records,
         results,
         plans,
         has_record_values?,
         has_after_terminal?
       ) do
    flow_terminal_pipeline_prepare(
      state,
      op,
      rest,
      virtual_records,
      [{:error, "ERR flow id must be a non-empty string"} | results],
      plans,
      has_record_values?,
      has_after_terminal?
    )
  end

  defp flow_terminal_pipeline_prepare_one(_state, _op, nil, _attrs, _now_ms),
    do: {:error, "ERR flow not found"}

  defp flow_terminal_pipeline_prepare_one(_state, :complete, record, attrs, now_ms) do
    case flow_prepare_complete_existing_record(
           record,
           attrs,
           Map.get(attrs, :lease_token),
           now_ms
         ) do
      {:ok, record, next} -> {:ok, next, {record, next, attrs}}
      {:error, _reason} = error -> error
    end
  end

  defp flow_terminal_pipeline_prepare_one(state, :retry, record, attrs, now_ms) do
    case flow_prepare_retry_existing_record(
           state,
           record,
           attrs,
           Map.get(attrs, :lease_token),
           now_ms
         ) do
      {:ok, record, next, history_meta} -> {:ok, next, {record, next, history_meta, attrs}}
      {:error, _reason} = error -> error
    end
  end

  defp flow_terminal_pipeline_prepare_one(_state, :fail, record, attrs, now_ms) do
    case flow_prepare_fail_existing_record(record, attrs, Map.get(attrs, :lease_token), now_ms) do
      {:ok, record, next} -> {:ok, next, {record, next, attrs}}
      {:error, _reason} = error -> error
    end
  end

  defp flow_terminal_pipeline_prepare_one(_state, :cancel, record, attrs, now_ms) do
    case flow_prepare_cancel_existing_record(record, attrs, now_ms) do
      {:ok, record, next} -> {:ok, next, {record, next, attrs}}
      {:error, _reason} = error -> error
    end
  end

  defp flow_terminal_pipeline_apply(_state, _op, [], _has_record_values?, _has_after_terminal?),
    do: :ok

  defp flow_terminal_pipeline_apply(
         state,
         :complete,
         plans,
         has_record_values?,
         has_after_terminal?
       ),
       do: flow_complete_many_apply(state, plans, has_record_values?, has_after_terminal?)

  defp flow_terminal_pipeline_apply(
         state,
         :retry,
         plans,
         has_record_values?,
         has_after_terminal?
       ),
       do: flow_retry_many_apply(state, plans, has_record_values?, has_after_terminal?)

  defp flow_terminal_pipeline_apply(state, :fail, plans, has_record_values?, has_after_terminal?),
    do: flow_fail_many_apply(state, plans, has_record_values?, has_after_terminal?)

  defp flow_terminal_pipeline_apply(
         state,
         :cancel,
         plans,
         has_record_values?,
         has_after_terminal?
       ),
       do: flow_cancel_many_apply(state, plans, has_record_values?, has_after_terminal?)

  defp do_flow_transition(
         state,
         %{id: id, from_state: from_state, to_state: to_state} = attrs
       ) do
    now_ms = flow_attrs_now_ms(attrs)
    run_at_ms = Map.get(attrs, :run_at_ms, now_ms)
    partition_key = Map.get(attrs, :partition_key)

    with {:ok, record, next} <-
           flow_prepare_transition_record(
             state,
             attrs,
             id,
             from_state,
             to_state,
             run_at_ms,
             now_ms
           ),
         :ok <- flow_apply_transition(state, record, next, partition_key, now_ms, attrs) do
      :ok
    end
  end

  defp do_flow_transition_many(state, %{records: [_ | _] = attrs_list}) do
    with :ok <- flow_many_partitions_valid?(state, attrs_list),
         :ok <- flow_transition_many_unique?(attrs_list),
         {:ok, plans, value_mode} <- flow_transition_many_prepare(state, attrs_list),
         :ok <- flow_transition_many_apply(state, plans, value_mode) do
      :ok
    end
  end

  defp do_flow_transition_many(_state, _attrs),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp flow_prepare_transition_record(
         state,
         attrs,
         id,
         from_state,
         to_state,
         run_at_ms,
         now_ms
       ) do
    partition_key = Map.get(attrs, :partition_key)

    with {:ok, record} <- flow_require_record(state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_transition_existing_record(
             record,
             attrs,
             id,
             from_state,
             to_state,
             run_at_ms,
             now_ms
           ) do
      {:ok, record, next}
    end
  end

  defp flow_prepare_transition_existing_record(
         nil,
         _attrs,
         _id,
         _from_state,
         _to_state,
         _run_at_ms,
         _now_ms
       ),
       do: {:error, "ERR flow not found"}

  defp flow_prepare_transition_existing_record(
         record,
         attrs,
         _id,
         from_state,
         to_state,
         run_at_ms,
         now_ms
       ) do
    with id when is_binary(id) <- Map.get(record, :id),
         :ok <- flow_require_expected_state(record, from_state),
         :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)),
         :ok <- flow_require_transition_lease(record, Map.get(attrs, :lease_token)),
         :ok <- flow_reject_terminal_transition(to_state) do
      version = Map.fetch!(record, :version) + 1
      partition_key = Map.get(record, :partition_key)

      with {:ok, value_refs} <- flow_named_value_refs(record, attrs, id, version, partition_key) do
        next =
          %{
            record
            | state: to_state,
              version: version,
              updated_at_ms: now_ms,
              next_run_at_ms: run_at_ms,
              priority: Map.get(attrs, :priority) || Map.get(record, :priority, 0),
              payload_ref:
                flow_value_ref(
                  attrs,
                  :payload,
                  id,
                  version,
                  partition_key,
                  Map.get(record, :payload_ref)
                ),
              ttl_ms: nil,
              retention_ttl_ms: Map.get(record, :retention_ttl_ms),
              history_hot_max_events: Map.get(record, :history_hot_max_events),
              history_max_events: Map.get(record, :history_max_events),
              lease_owner: nil,
              lease_token: nil,
              lease_deadline_ms: 0
          }
          |> flow_put_record_value_refs(value_refs)
          |> flow_stamp_terminal_retention(now_ms)

        with :ok <- flow_validate_record_keys(next) do
          {:ok, record, next}
        end
      end
    else
      {:error, _reason} = error -> error
      _ -> {:error, "ERR flow not found"}
    end
  end

  defp flow_reject_terminal_transition(to_state) do
    if Ferricstore.Flow.LMDB.terminal_state?(to_state) do
      {:error, "ERR terminal flow state requires FLOW.COMPLETE, FLOW.FAIL, or FLOW.CANCEL"}
    else
      :ok
    end
  end

  defp flow_apply_transition(state, record, next, partition_key, now_ms, attrs) do
    plans = [{record, next}]

    with :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_transition_move_indexes(state, plans),
         :ok <-
           flow_put_state_record(
             state,
             FlowKeys.state_key(next.id, partition_key),
             next
           ),
         :ok <- flow_history_put_planned(state, record, next, "transitioned", now_ms),
         :ok <- flow_after_history_put(state, next) do
      :ok
    end
  end

  defp flow_transition_many_unique?(attrs_list) do
    {_seen, result} =
      Enum.reduce_while(attrs_list, {MapSet.new(), :ok}, fn %{id: id}, {seen, :ok} ->
        if MapSet.member?(seen, id) do
          {:halt, {seen, {:error, "ERR flow duplicate id in batch"}}}
        else
          {:cont, {MapSet.put(seen, id), :ok}}
        end
      end)

    result
  end

  defp flow_transition_many_prepare(state, attrs_list) do
    existing_records = flow_read_records(state, attrs_list)

    attrs_list
    |> Enum.zip(existing_records)
    |> Enum.reduce_while({:ok, [], :empty}, fn
      {%{id: id, from_state: from_state, to_state: to_state} = attrs, existing},
      {:ok, acc, value_mode} ->
        now_ms = flow_attrs_now_ms(attrs)
        run_at_ms = Map.get(attrs, :run_at_ms, now_ms)

        case flow_prepare_transition_existing_record(
               existing,
               attrs,
               id,
               from_state,
               to_state,
               run_at_ms,
               now_ms
             ) do
          {:ok, record, next} ->
            {:cont,
             {:ok, [{record, next, attrs} | acc],
              flow_merge_record_value_mode(value_mode, flow_attrs_record_value_mode(attrs))}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      {_bad, _existing}, {:ok, _acc, _value_mode} ->
        {:halt, {:error, "ERR flow id must be a non-empty string"}}
    end)
    |> case do
      {:ok, plans, value_mode} ->
        {:ok, Enum.reverse(plans), flow_finalize_record_value_mode(value_mode)}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_transition_many_apply(state, plans, value_mode) do
    with :ok <- flow_transition_many_put_record_values(state, plans, value_mode),
         :ok <- flow_transition_move_indexes(state, plans),
         :ok <- flow_claim_put_state_records(state, plans),
         :ok <- flow_transition_put_history(state, plans) do
      :ok
    end
  end

  defp flow_transition_many_put_record_values(_state, _plans, :none), do: :ok

  defp flow_transition_many_put_record_values(state, plans, :payload_only) do
    flow_transition_many_put_payloads(state, plans)
  end

  defp flow_transition_many_put_record_values(state, plans, :mixed) do
    flow_many_put_record_values(state, plans, true)
  end

  defp flow_transition_many_put_record_values(state, plans, :unknown) do
    if flow_transition_many_payload_only?(plans) do
      flow_transition_many_put_payloads(state, plans)
    else
      flow_many_put_record_values(state, plans)
    end
  end

  defp flow_transition_many_payload_only?(plans) do
    Enum.all?(plans, fn
      {_record, _next, attrs} ->
        Map.has_key?(attrs, :payload) and not Map.has_key?(attrs, :result) and
          not Map.has_key?(attrs, :error) and
          map_size(flow_named_values(Map.get(attrs, :values))) == 0
    end)
  end

  defp flow_transition_many_put_payloads(state, plans) do
    Enum.reduce_while(plans, :ok, fn {_record, next, %{payload: payload}}, :ok ->
      key = Map.fetch!(next, :payload_ref)

      case flow_put_record_payload_value(state, next, key, payload) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_put_record_payload_value(state, record, key, payload) do
    with :ok <- flow_validate_key_size(key) do
      raw_put_cold(
        state,
        key,
        Flow.encode_value(payload),
        flow_record_expire_at(record)
      )
    end
  end

  defp do_flow_retry(state, %{id: id, lease_token: lease_token} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)

    with {:ok, record} <- flow_require_record(state, id, partition_key),
         {:ok, record, next, history_meta} <-
           flow_prepare_retry_existing_record(state, record, attrs, lease_token, now_ms),
         :ok <- flow_apply_retry(state, record, next, partition_key, now_ms, history_meta, attrs) do
      :ok
    end
  end

  defp do_flow_retry_many(state, %{records: [_ | _] = attrs_list}) do
    with :ok <- flow_many_partitions_valid?(state, attrs_list),
         :ok <- flow_transition_many_unique?(attrs_list),
         {:ok, plans, has_record_values?, has_after_terminal?} <-
           flow_retry_many_prepare(state, attrs_list),
         :ok <- flow_retry_many_apply(state, plans, has_record_values?, has_after_terminal?) do
      :ok
    end
  end

  defp do_flow_retry_many(_state, _attrs),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp flow_prepare_retry_existing_record(state, record, attrs, lease_token, now_ms) do
    with :ok <- flow_require_running_lease(record, lease_token),
         :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)) do
      retry_policy = flow_retry_policy_for_record(state, record, attrs)
      next_attempts = Map.get(record, :attempts, 0) + 1
      version = Map.fetch!(record, :version) + 1
      id = Map.fetch!(record, :id)
      partition_key = Map.get(record, :partition_key)

      {next_state, next_run_at_ms, retry_decision} =
        flow_retry_next_state(record, attrs, retry_policy, next_attempts, now_ms)

      payload_ref =
        flow_value_ref(
          attrs,
          :payload,
          id,
          version,
          partition_key,
          Map.get(record, :payload_ref)
        )

      error_ref = flow_value_ref(attrs, :error, id, version, partition_key)

      with {:ok, value_refs} <- flow_named_value_refs(record, attrs, id, version, partition_key) do
        next =
          %{
            record
            | state: next_state,
              version: version,
              attempts: next_attempts,
              updated_at_ms: now_ms,
              next_run_at_ms: next_run_at_ms,
              payload_ref: payload_ref,
              error_ref: error_ref,
              ttl_ms: nil,
              retention_ttl_ms: Map.get(record, :retention_ttl_ms),
              lease_owner: nil,
              lease_token: nil,
              lease_deadline_ms: 0,
              run_state: nil
          }
          |> flow_put_record_value_refs(value_refs)
          |> flow_stamp_terminal_retention(now_ms)

        with :ok <- flow_validate_claim_next_record_keys(next) do
          {:ok, record, next, flow_retry_history_meta(record, next, retry_policy, retry_decision)}
        end
      end
    end
  end

  defp flow_retry_next_state(record, attrs, retry_policy, next_attempts, now_ms) do
    if RetryPolicy.attempt_allowed?(retry_policy, next_attempts) do
      run_at_ms =
        Map.get(attrs, :run_at_ms) ||
          RetryPolicy.next_run_at_ms(retry_policy, Map.fetch!(record, :id), next_attempts, now_ms)

      {flow_retry_run_state(record), run_at_ms, "scheduled"}
    else
      exhausted_to = Map.fetch!(retry_policy, :exhausted_to)

      next_run_at_ms =
        if Ferricstore.Flow.LMDB.terminal_state?(exhausted_to), do: nil, else: now_ms

      {exhausted_to, next_run_at_ms, "exhausted"}
    end
  end

  defp flow_retry_history_meta(record, next, retry_policy, retry_decision) do
    backoff = Map.fetch!(retry_policy, :backoff)

    %{
      "retry_decision" => retry_decision,
      "retry_run_state" => flow_retry_run_state(record),
      "retry_next_run_at_ms" => Map.get(next, :next_run_at_ms),
      "retry_max_retries" => Map.get(retry_policy, :max_retries),
      "retry_backoff_kind" => Map.get(backoff, :kind),
      "retry_backoff_base_ms" => Map.get(backoff, :base_ms),
      "retry_backoff_max_ms" => Map.get(backoff, :max_ms),
      "retry_jitter_pct" => Map.get(backoff, :jitter_pct),
      "retry_exhausted_to" => Map.get(retry_policy, :exhausted_to)
    }
  end

  defp flow_retry_policy_for_record(state, record, attrs) do
    run_state = flow_retry_run_state(record)
    flow_policy = flow_read_policy(state, Map.get(record, :type))
    RetryPolicy.resolve(flow_policy, run_state, Map.get(attrs, :retry_policy))
  end

  defp flow_retry_run_state(record) do
    case Map.get(record, :run_state) do
      state when is_binary(state) and state != "" -> state
      _ -> "queued"
    end
  end

  defp flow_apply_retry(state, record, next, partition_key, now_ms, history_meta, attrs) do
    plans = [{record, next}]

    with :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_transition_move_indexes(state, plans),
         :ok <-
           flow_put_state_record(
             state,
             FlowKeys.state_key(next.id, partition_key),
             next
           ),
         :ok <- flow_history_put_planned(state, record, next, "retry", now_ms, history_meta),
         :ok <- flow_after_history_put(state, next),
         :ok <- flow_maybe_after_retry_terminal(state, next, now_ms) do
      :ok
    end
  end

  defp flow_retry_many_prepare(state, attrs_list) do
    existing_records = flow_read_records(state, attrs_list)

    attrs_list
    |> Enum.zip(existing_records)
    |> Enum.reduce_while({:ok, [], false, false}, fn
      {%{id: _id, lease_token: lease_token} = attrs, existing},
      {:ok, acc, has_values?, has_after_terminal?} ->
        now_ms = flow_attrs_now_ms(attrs)

        case existing do
          nil ->
            {:halt, {:error, "ERR flow not found"}}

          record ->
            case flow_prepare_retry_existing_record(state, record, attrs, lease_token, now_ms) do
              {:ok, record, next, history_meta} ->
                {:cont,
                 {:ok, [{record, next, history_meta, attrs} | acc],
                  has_values? or flow_attrs_have_record_values?(attrs),
                  has_after_terminal? or flow_terminal_after_required?(:retry, next)}}

              {:error, _reason} = error ->
                {:halt, error}
            end
        end

      {_bad, _existing}, {:ok, _acc, _has_values?, _has_after_terminal?} ->
        {:halt, {:error, "ERR flow id must be a non-empty string"}}
    end)
    |> case do
      {:ok, plans, has_record_values?, has_after_terminal?} ->
        {:ok, Enum.reverse(plans), has_record_values?, has_after_terminal?}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_retry_many_apply(state, plans, has_record_values?, has_after_terminal?) do
    with :ok <- flow_many_put_record_values(state, plans, has_record_values?),
         :ok <- flow_transition_move_indexes(state, plans),
         :ok <- flow_claim_put_state_records(state, plans),
         :ok <- flow_retry_many_put_history(state, plans),
         :ok <- flow_many_after_retry_terminal(state, plans, has_after_terminal?) do
      :ok
    end
  end

  defp flow_maybe_after_retry_terminal(state, next, now_ms) do
    status = Map.get(next, :state)

    if Ferricstore.Flow.LMDB.terminal_state?(status) do
      with :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms) do
        flow_maybe_apply_child_terminal(state, next, status, now_ms)
      end
    else
      :ok
    end
  end

  defp flow_many_after_retry_terminal(_state, _plans, false), do: :ok

  defp flow_many_after_retry_terminal(state, plans, true) do
    flow_many_after_retry_terminal(state, plans)
  end

  defp flow_many_after_retry_terminal(state, plans) do
    Enum.reduce_while(plans, :ok, fn plan, :ok ->
      {_record, next} = flow_claim_plan_pair(plan)
      now_ms = flow_record_updated_at_ms(next)

      case flow_maybe_after_retry_terminal(state, next, now_ms) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp do_flow_fail(state, %{id: id, lease_token: lease_token} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)

    with {:ok, record} <- flow_require_record(state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms),
         :ok <- flow_apply_fail(state, record, next, partition_key, now_ms, attrs) do
      :ok
    end
  end

  defp do_flow_fail_many(state, %{records: [_ | _] = attrs_list}) do
    with :ok <- flow_many_partitions_valid?(state, attrs_list),
         :ok <- flow_transition_many_unique?(attrs_list),
         {:ok, plans, has_record_values?, has_after_terminal?} <-
           flow_fail_many_prepare(state, attrs_list),
         :ok <- flow_fail_many_apply(state, plans, has_record_values?, has_after_terminal?) do
      :ok
    end
  end

  defp do_flow_fail_many(_state, _attrs),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms) do
    with :ok <- flow_require_running_lease(record, lease_token),
         :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)) do
      version = Map.fetch!(record, :version) + 1
      id = Map.fetch!(record, :id)
      partition_key = Map.get(record, :partition_key)

      payload_ref =
        flow_value_ref(
          attrs,
          :payload,
          id,
          version,
          partition_key,
          Map.get(record, :payload_ref)
        )

      error_ref = flow_value_ref(attrs, :error, id, version, partition_key)
      retention_ttl_ms = Map.get(attrs, :ttl_ms) || Map.get(record, :retention_ttl_ms)

      with {:ok, value_refs} <- flow_named_value_refs(record, attrs, id, version, partition_key) do
        next =
          %{
            record
            | state: "failed",
              version: version,
              updated_at_ms: now_ms,
              payload_ref: payload_ref,
              error_ref: error_ref,
              ttl_ms: nil,
              retention_ttl_ms: retention_ttl_ms,
              lease_owner: nil,
              lease_token: nil,
              lease_deadline_ms: 0,
              next_run_at_ms: nil
          }
          |> flow_put_record_value_refs(value_refs)
          |> flow_stamp_terminal_retention(now_ms)

        with :ok <- flow_validate_terminal_state_index_key(next) do
          {:ok, record, next}
        end
      end
    end
  end

  defp flow_apply_fail(state, record, next, partition_key, now_ms, attrs) do
    plans = [{record, next}]

    with :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_transition_move_indexes(state, plans),
         :ok <-
           flow_put_state_record(
             state,
             FlowKeys.state_key(next.id, partition_key),
             next
           ),
         :ok <- flow_history_put_planned(state, record, next, "failed", now_ms),
         :ok <- flow_after_history_put(state, next),
         :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms),
         :ok <- flow_maybe_apply_child_terminal(state, next, "failed", now_ms) do
      :ok
    end
  end

  defp flow_fail_many_prepare(state, attrs_list) do
    existing_records = flow_read_records(state, attrs_list)

    attrs_list
    |> Enum.zip(existing_records)
    |> Enum.reduce_while({:ok, [], false, false}, fn
      {%{id: _id, lease_token: lease_token} = attrs, existing},
      {:ok, acc, has_values?, has_after_terminal?} ->
        now_ms = flow_attrs_now_ms(attrs)

        case existing do
          nil ->
            {:halt, {:error, "ERR flow not found"}}

          record ->
            case flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms) do
              {:ok, record, next} ->
                {:cont,
                 {:ok, [{record, next, attrs} | acc],
                  has_values? or flow_attrs_have_record_values?(attrs),
                  has_after_terminal? or flow_terminal_after_required?(:fail, next)}}

              {:error, _reason} = error ->
                {:halt, error}
            end
        end

      {_bad, _existing}, {:ok, _acc, _has_values?, _has_after_terminal?} ->
        {:halt, {:error, "ERR flow id must be a non-empty string"}}
    end)
    |> case do
      {:ok, plans, has_record_values?, has_after_terminal?} ->
        {:ok, Enum.reverse(plans), has_record_values?, has_after_terminal?}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_fail_many_apply(state, plans, has_record_values?, has_after_terminal?) do
    with :ok <- flow_many_put_record_values(state, plans, has_record_values?),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <- flow_claim_put_state_records(state, plans),
         :ok <- flow_many_put_history(state, plans, "failed"),
         :ok <- flow_many_after_terminal(state, plans, "failed", has_after_terminal?) do
      :ok
    end
  end

  defp do_flow_cancel(state, %{id: id} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)

    with {:ok, record} <- flow_require_record(state, id, partition_key),
         {:ok, record, next} <- flow_prepare_cancel_existing_record(record, attrs, now_ms),
         :ok <- flow_apply_cancel(state, record, next, attrs, partition_key, now_ms) do
      :ok
    end
  end

  defp do_flow_cancel_many(state, %{records: [_ | _] = attrs_list}) do
    with :ok <- flow_many_partitions_valid?(state, attrs_list),
         :ok <- flow_transition_many_unique?(attrs_list),
         {:ok, plans, has_record_values?, has_after_terminal?} <-
           flow_cancel_many_prepare(state, attrs_list),
         :ok <- flow_cancel_many_apply(state, plans, has_record_values?, has_after_terminal?) do
      :ok
    end
  end

  defp do_flow_cancel_many(_state, _attrs),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp flow_prepare_cancel_existing_record(record, attrs, now_ms) do
    with :ok <- flow_require_fencing_token(record, Map.fetch!(attrs, :fencing_token)),
         :ok <- flow_require_transition_lease(record, Map.get(attrs, :lease_token)) do
      version = Map.fetch!(record, :version) + 1
      partition_key = Map.get(record, :partition_key)

      error_ref =
        flow_value_ref(
          attrs,
          :error,
          Map.fetch!(record, :id),
          version,
          partition_key,
          Map.get(attrs, :reason_ref)
        )

      retention_ttl_ms = Map.get(attrs, :ttl_ms) || Map.get(record, :retention_ttl_ms)

      with {:ok, value_refs} <-
             flow_named_value_refs(
               record,
               attrs,
               Map.fetch!(record, :id),
               version,
               partition_key
             ) do
        next =
          %{
            record
            | state: "cancelled",
              version: version,
              updated_at_ms: now_ms,
              error_ref: error_ref,
              ttl_ms: nil,
              retention_ttl_ms: retention_ttl_ms,
              lease_owner: nil,
              lease_token: nil,
              lease_deadline_ms: 0,
              next_run_at_ms: nil
          }
          |> flow_put_record_value_refs(value_refs)
          |> flow_stamp_terminal_retention(now_ms)

        with :ok <- flow_validate_terminal_state_index_key(next) do
          {:ok, record, next}
        end
      end
    end
  end

  defp flow_apply_cancel(state, record, next, attrs, partition_key, now_ms) do
    plans = [{record, next}]
    refresh_attrs = flow_cancel_refresh_attrs(attrs)

    with :ok <- do_flow_put_record_values(state, next, attrs),
         :ok <- flow_refresh_terminal_value_expirations(state, next, refresh_attrs),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <-
           flow_put_state_record(
             state,
             FlowKeys.state_key(next.id, partition_key),
             next
           ),
         :ok <- flow_history_put_planned(state, record, next, "cancelled", now_ms),
         :ok <- flow_after_history_put(state, next),
         :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms),
         :ok <- flow_maybe_apply_child_terminal(state, next, "cancelled", now_ms) do
      :ok
    end
  end

  defp do_flow_cross_terminal(state, :complete, %{id: id, lease_token: lease_token} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_complete_existing_record(record, attrs, lease_token, now_ms),
         :ok <- flow_apply_complete_local(child_state, record, next, partition_key, now_ms, attrs),
         :ok <- flow_apply_child_terminal_chain(state, next, "completed", now_ms) do
      :ok
    end
  end

  defp do_flow_cross_terminal(state, :fail, %{id: id, lease_token: lease_token} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms),
         :ok <- flow_apply_fail_local(child_state, record, next, partition_key, now_ms, attrs),
         :ok <- flow_apply_child_terminal_chain(state, next, "failed", now_ms) do
      :ok
    end
  end

  defp do_flow_cross_terminal(state, :retry, %{id: id, lease_token: lease_token} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next, history_meta} <-
           flow_prepare_retry_existing_record(child_state, record, attrs, lease_token, now_ms),
         :ok <-
           flow_apply_retry_local(
             child_state,
             record,
             next,
             partition_key,
             now_ms,
             history_meta,
             attrs
           ),
         :ok <- flow_maybe_apply_cross_terminal_chain(state, next, now_ms) do
      :ok
    end
  end

  defp do_flow_cross_terminal(state, :cancel, %{id: id} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <- flow_prepare_cancel_existing_record(record, attrs, now_ms),
         :ok <- flow_apply_cancel_local(child_state, record, next, attrs, partition_key, now_ms),
         :ok <- flow_apply_child_terminal_chain(state, next, "cancelled", now_ms) do
      :ok
    end
  end

  defp do_flow_cross_terminal_many(state, op, %{records: [_ | _] = attrs_list})
       when op in [:complete, :retry, :fail, :cancel] do
    with :ok <- flow_transition_many_unique?(attrs_list),
         {:ok, plans} <- flow_cross_terminal_many_prepare(state, op, attrs_list),
         :ok <- flow_cross_terminal_many_apply(state, op, plans) do
      :ok
    end
  end

  defp do_flow_cross_terminal_many(state, op, [_ | _] = attrs_list)
       when op in [:complete, :retry, :fail, :cancel] do
    with :ok <- flow_transition_many_unique?(attrs_list),
         {:ok, plans} <- flow_cross_terminal_many_prepare(state, op, attrs_list),
         :ok <- flow_cross_terminal_many_apply(state, op, plans) do
      :ok
    end
  end

  defp do_flow_cross_terminal_many(_state, _op, _attrs),
    do: {:error, "ERR flow items must be a non-empty list"}

  defp flow_cross_terminal_many_prepare(state, op, attrs_list) do
    attrs_list
    |> Enum.reduce_while({:ok, []}, fn attrs, {:ok, acc} ->
      case flow_cross_terminal_prepare(state, op, attrs) do
        {:ok, plan} -> {:cont, {:ok, [plan | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, plans} -> {:ok, Enum.reverse(plans)}
      {:error, _reason} = error -> error
    end
  end

  defp flow_cross_terminal_prepare(state, :complete, %{id: id, lease_token: lease_token} = attrs)
       when is_binary(id) and id != "" do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_complete_existing_record(record, attrs, lease_token, now_ms) do
      {:ok, {child_state, record, next, attrs, partition_key, now_ms}}
    end
  end

  defp flow_cross_terminal_prepare(state, :fail, %{id: id, lease_token: lease_token} = attrs)
       when is_binary(id) and id != "" do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <-
           flow_prepare_fail_existing_record(record, attrs, lease_token, now_ms) do
      {:ok, {child_state, record, next, attrs, partition_key, now_ms}}
    end
  end

  defp flow_cross_terminal_prepare(state, :retry, %{id: id, lease_token: lease_token} = attrs)
       when is_binary(id) and id != "" do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next, history_meta} <-
           flow_prepare_retry_existing_record(child_state, record, attrs, lease_token, now_ms) do
      {:ok, {child_state, record, next, attrs, partition_key, now_ms, history_meta}}
    end
  end

  defp flow_cross_terminal_prepare(state, :cancel, %{id: id} = attrs)
       when is_binary(id) and id != "" do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)
    child_state = cross_shard_state_for_key(state, FlowKeys.state_key(id, partition_key))

    with {:ok, record} <- flow_require_record(child_state, id, partition_key),
         {:ok, record, next} <- flow_prepare_cancel_existing_record(record, attrs, now_ms) do
      {:ok, {child_state, record, next, attrs, partition_key, now_ms}}
    end
  end

  defp flow_cross_terminal_prepare(_state, _op, _attrs),
    do: {:error, "ERR flow id must be a non-empty string"}

  defp flow_cross_terminal_many_apply(state, op, plans) do
    Enum.reduce_while(plans, :ok, fn plan, :ok ->
      case flow_cross_terminal_apply_plan(state, op, plan) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_cross_terminal_apply_plan(
         state,
         :complete,
         {child_state, record, next, attrs, partition_key, now_ms}
       ) do
    with :ok <- flow_apply_complete_local(child_state, record, next, partition_key, now_ms, attrs) do
      flow_apply_child_terminal_chain(state, next, "completed", now_ms)
    end
  end

  defp flow_cross_terminal_apply_plan(
         state,
         :fail,
         {child_state, record, next, attrs, partition_key, now_ms}
       ) do
    with :ok <- flow_apply_fail_local(child_state, record, next, partition_key, now_ms, attrs) do
      flow_apply_child_terminal_chain(state, next, "failed", now_ms)
    end
  end

  defp flow_cross_terminal_apply_plan(
         state,
         :retry,
         {child_state, record, next, attrs, partition_key, now_ms, history_meta}
       ) do
    with :ok <-
           flow_apply_retry_local(
             child_state,
             record,
             next,
             partition_key,
             now_ms,
             history_meta,
             attrs
           ) do
      flow_maybe_apply_cross_terminal_chain(state, next, now_ms)
    end
  end

  defp flow_cross_terminal_apply_plan(
         state,
         :cancel,
         {child_state, record, next, attrs, partition_key, now_ms}
       ) do
    with :ok <- flow_apply_cancel_local(child_state, record, next, attrs, partition_key, now_ms) do
      flow_apply_child_terminal_chain(state, next, "cancelled", now_ms)
    end
  end

  defp flow_apply_complete_local(state, record, next, partition_key, now_ms, attrs) do
    plans = [{record, next}]

    with :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <- flow_put_state_record(state, FlowKeys.state_key(next.id, partition_key), next),
         :ok <- flow_history_put_planned(state, record, next, "completed", now_ms),
         :ok <- flow_after_history_put(state, next) do
      flow_maybe_cancel_children_on_parent_closed(state, next, now_ms)
    end
  end

  defp flow_apply_fail_local(state, record, next, partition_key, now_ms, attrs) do
    plans = [{record, next}]

    with :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <- flow_put_state_record(state, FlowKeys.state_key(next.id, partition_key), next),
         :ok <- flow_history_put_planned(state, record, next, "failed", now_ms),
         :ok <- flow_after_history_put(state, next) do
      flow_maybe_cancel_children_on_parent_closed(state, next, now_ms)
    end
  end

  defp flow_apply_cancel_local(state, record, next, attrs, partition_key, now_ms) do
    plans = [{record, next}]
    refresh_attrs = flow_cancel_refresh_attrs(attrs)

    with :ok <- do_flow_put_record_values(state, next, attrs),
         :ok <- flow_refresh_terminal_value_expirations(state, next, refresh_attrs),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <- flow_put_state_record(state, FlowKeys.state_key(next.id, partition_key), next),
         :ok <- flow_history_put_planned(state, record, next, "cancelled", now_ms),
         :ok <- flow_after_history_put(state, next) do
      flow_maybe_cancel_children_on_parent_closed(state, next, now_ms)
    end
  end

  defp flow_apply_retry_local(state, record, next, partition_key, now_ms, history_meta, attrs) do
    plans = [{record, next}]

    with :ok <- flow_put_record_values(state, next, attrs),
         :ok <- flow_transition_move_indexes(state, plans),
         :ok <- flow_put_state_record(state, FlowKeys.state_key(next.id, partition_key), next),
         :ok <- flow_history_put_planned(state, record, next, "retry", now_ms, history_meta) do
      flow_after_history_put(state, next)
    end
  end

  defp flow_maybe_apply_cross_terminal_chain(state, next, now_ms) do
    status = Map.get(next, :state)

    if Ferricstore.Flow.LMDB.terminal_state?(status) do
      with :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms) do
        flow_apply_child_terminal_chain(state, next, status, now_ms)
      end
    else
      :ok
    end
  end

  defp flow_apply_child_terminal_chain(state, child, status, now_ms) do
    parent_id = Map.get(child, :parent_flow_id)
    parent_partition = Map.get(child, :parent_partition_key) || Map.get(child, :partition_key)

    if is_binary(parent_id) and parent_id != "" and status in ["completed", "failed", "cancelled"] do
      parent_state =
        cross_shard_state_for_key(state, FlowKeys.state_key(parent_id, parent_partition))

      case flow_read_record(parent_state, parent_id, parent_partition) do
        nil ->
          :ok

        parent ->
          case flow_child_terminal_parent_next(parent, child, status, now_ms) do
            {:ok, nil} ->
              :ok

            {:ok, next_parent} ->
              with :ok <-
                     flow_apply_parent_update(
                       parent_state,
                       parent,
                       next_parent,
                       "child_#{status}",
                       now_ms
                     ),
                   :ok <-
                     flow_maybe_cancel_children_on_parent_closed(
                       parent_state,
                       next_parent,
                       now_ms
                     ) do
                flow_maybe_apply_resolved_parent_terminal_cross(state, next_parent, now_ms)
              end
          end
      end
    else
      :ok
    end
  end

  defp flow_maybe_apply_resolved_parent_terminal_cross(state, parent, now_ms) do
    case Map.get(parent, :state) do
      "completed" -> flow_apply_child_terminal_chain(state, parent, "completed", now_ms)
      "failed" -> flow_apply_child_terminal_chain(state, parent, "failed", now_ms)
      "cancelled" -> flow_apply_child_terminal_chain(state, parent, "cancelled", now_ms)
      _state -> :ok
    end
  end

  defp flow_maybe_cancel_children_on_parent_closed(state, parent, now_ms) do
    case flow_child_groups(parent) do
      groups when map_size(groups) == 0 ->
        :ok

      groups ->
        if Ferricstore.Flow.LMDB.terminal_state?(Map.get(parent, :state)) or
             flow_has_resolved_cancel_child_group?(groups) do
          flow_cancel_children_on_parent_closed(state, parent, now_ms, groups)
        else
          :ok
        end
    end
  end

  defp flow_has_resolved_cancel_child_group?(groups) do
    Enum.any?(groups, fn {_group_id, group} ->
      not is_nil(Map.get(group, "resolved")) and flow_group_should_cancel_children?(group)
    end)
  end

  defp flow_cancel_children_on_parent_closed(state, parent, now_ms, groups) do
    {updated_groups, child_refs} =
      Enum.reduce(groups, {groups, []}, fn {group_id, group}, {groups_acc, child_acc} ->
        if flow_group_should_cancel_children?(group) do
          running_refs = flow_group_running_child_refs(group, Map.get(parent, :partition_key))
          running_ids = Enum.map(running_refs, fn {child_id, _partition_key} -> child_id end)
          updated_group = flow_group_mark_children_cancelled(group, running_ids)
          {Map.put(groups_acc, group_id, updated_group), running_refs ++ child_acc}
        else
          {groups_acc, child_acc}
        end
      end)

    child_refs = Enum.uniq(child_refs)

    if child_refs == [] do
      :ok
    else
      with :ok <- flow_cancel_direct_children(state, child_refs, now_ms),
           {:ok, updated_parent} <-
             flow_parent_with_updated_child_groups(parent, updated_groups, now_ms) do
        flow_apply_parent_update(state, parent, updated_parent, "children_cancelled", now_ms)
      end
    end
  end

  defp flow_group_should_cancel_children?(%{"on_parent_closed" => "cancel_children"} = group) do
    group
    |> Map.get("children", %{})
    |> Enum.any?(fn {_child_id, status} -> status == "running" end)
  end

  defp flow_group_should_cancel_children?(_group), do: false

  defp flow_group_running_child_refs(group, default_partition_key) do
    child_partitions = Map.get(group, "child_partitions", %{})

    group
    |> Map.get("children", %{})
    |> Enum.flat_map(fn
      {child_id, "running"} ->
        [{child_id, Map.get(child_partitions, child_id, default_partition_key)}]

      _other ->
        []
    end)
  end

  defp flow_group_mark_children_cancelled(group, []), do: group

  defp flow_group_mark_children_cancelled(group, child_ids) do
    children =
      Enum.reduce(child_ids, Map.get(group, "children", %{}), fn child_id, acc ->
        Map.put(acc, child_id, "cancelled")
      end)

    summary = Map.get(group, "summary", %{})
    cancelled = Map.get(summary, "cancelled", 0) + length(child_ids)
    resolved = Map.get(group, "resolved") || "failure"

    group
    |> Map.put("children", children)
    |> Map.put("results", flow_child_group_cancelled_results(group, child_ids))
    |> Map.put("summary", Map.put(summary, "cancelled", cancelled))
    |> Map.put("resolved", resolved)
  end

  defp flow_child_group_cancelled_results(group, child_ids) do
    Enum.reduce(child_ids, Map.get(group, "results", %{}), fn child_id, acc ->
      Map.put(acc, child_id, %{"status" => "cancelled"})
    end)
  end

  defp flow_cancel_direct_children(state, child_refs, now_ms) do
    Enum.reduce_while(child_refs, :ok, fn {child_id, partition_key}, :ok ->
      child_state = flow_child_state_for_partition(state, child_id, partition_key)

      case flow_read_record(child_state, child_id, partition_key) do
        nil ->
          {:cont, :ok}

        child ->
          if Ferricstore.Flow.LMDB.terminal_state?(Map.get(child, :state)) do
            {:cont, :ok}
          else
            case flow_apply_internal_child_cancel(child_state, child, now_ms) do
              :ok -> {:cont, :ok}
              {:error, _reason} = error -> {:halt, error}
            end
          end
      end
    end)
  end

  defp flow_child_state_for_partition(state, child_id, partition_key) do
    if cross_shard_pending_active?() do
      cross_shard_state_for_key(state, FlowKeys.state_key(child_id, partition_key))
    else
      state
    end
  end

  defp flow_apply_internal_child_cancel(state, child, now_ms) do
    next =
      child
      |> Map.merge(%{
        state: "cancelled",
        version: Map.fetch!(child, :version) + 1,
        updated_at_ms: now_ms,
        ttl_ms: nil,
        lease_owner: nil,
        lease_token: nil,
        lease_deadline_ms: 0,
        next_run_at_ms: nil
      })
      |> flow_stamp_terminal_retention(now_ms)

    with :ok <- flow_transition_move_indexes(state, [{child, next}]),
         :ok <-
           flow_put_state_record(
             state,
             FlowKeys.state_key(next.id, Map.get(next, :partition_key)),
             next
           ),
         :ok <- flow_history_put_planned(state, child, next, "cancelled", now_ms),
         :ok <- flow_after_history_put(state, next) do
      flow_maybe_cancel_children_on_parent_closed(state, next, now_ms)
    end
  end

  defp flow_parent_with_updated_child_groups(parent, updated_groups, now_ms) do
    next =
      parent
      |> Map.merge(%{
        version: Map.fetch!(parent, :version) + 1,
        updated_at_ms: now_ms,
        child_groups: updated_groups
      })

    with :ok <- flow_validate_record_keys(next) do
      {:ok, next}
    end
  end

  defp flow_maybe_apply_child_terminal(
         state,
         %{parent_flow_id: parent_id, partition_key: partition_key} = child,
         status,
         now_ms
       )
       when is_binary(parent_id) and parent_id != "" and
              status in ["completed", "failed", "cancelled"] do
    parent_partition_key = Map.get(child, :parent_partition_key) || partition_key

    if parent_partition_key != partition_key and not cross_shard_pending_active?() do
      :ok
    else
      parent_state =
        if cross_shard_pending_active?() do
          cross_shard_state_for_key(state, FlowKeys.state_key(parent_id, parent_partition_key))
        else
          state
        end

      case flow_read_record(parent_state, parent_id, parent_partition_key) do
        nil ->
          :ok

        parent ->
          case flow_child_terminal_parent_next(parent, child, status, now_ms) do
            {:ok, nil} ->
              :ok

            {:ok, next_parent} ->
              with :ok <-
                     flow_apply_parent_update(
                       parent_state,
                       parent,
                       next_parent,
                       "child_#{status}",
                       now_ms
                     ),
                   :ok <-
                     flow_maybe_cancel_children_on_parent_closed(
                       parent_state,
                       next_parent,
                       now_ms
                     ) do
                flow_maybe_apply_resolved_parent_terminal(parent_state, next_parent, now_ms)
              end
          end
      end
    end
  end

  defp flow_maybe_apply_child_terminal(_state, _child, _status, _now_ms), do: :ok

  defp flow_many_after_terminal(_state, _plans, _status, false), do: :ok

  defp flow_many_after_terminal(state, plans, status, true) do
    flow_many_after_terminal(state, plans, status)
  end

  defp flow_many_after_terminal(state, plans, status) do
    Enum.reduce_while(plans, :ok, fn plan, :ok ->
      {_record, next} = flow_claim_plan_pair(plan)

      if flow_terminal_after_noop?(next) do
        {:cont, :ok}
      else
        now_ms = flow_record_updated_at_ms(next)

        with :ok <- flow_maybe_cancel_children_on_parent_closed(state, next, now_ms),
             :ok <- flow_maybe_apply_child_terminal(state, next, status, now_ms) do
          {:cont, :ok}
        else
          {:error, _reason} = error -> {:halt, error}
        end
      end
    end)
  end

  defp flow_terminal_after_required?(:retry, next) do
    Ferricstore.Flow.LMDB.terminal_state?(Map.get(next, :state)) and
      not flow_terminal_after_noop?(next)
  end

  defp flow_terminal_after_required?(_op, next), do: not flow_terminal_after_noop?(next)

  defp flow_terminal_after_noop?(record) do
    flow_blank_metadata?(Map.get(record, :parent_flow_id)) and
      flow_empty_child_groups?(Map.get(record, :child_groups))
  end

  defp flow_empty_child_groups?(groups) when is_map(groups), do: map_size(groups) == 0
  defp flow_empty_child_groups?(_groups), do: true

  defp flow_record_updated_at_ms(%{updated_at_ms: now_ms}) when is_integer(now_ms), do: now_ms
  defp flow_record_updated_at_ms(_record), do: apply_now_ms()

  defp flow_attrs_now_ms(%{now_ms: now_ms}), do: now_ms
  defp flow_attrs_now_ms(_attrs), do: apply_now_ms()

  defp flow_child_terminal_parent_next(parent, child, status, now_ms) do
    child_id = Map.fetch!(child, :id)
    groups = flow_child_groups(parent)

    case flow_find_open_child_group(groups, child_id) do
      nil ->
        {:ok, nil}

      {group_id, group} ->
        updated_group = flow_child_group_count_terminal(group, child, status)
        resolved_group = flow_child_group_resolve(updated_group, status)
        updated_groups = Map.put(groups, group_id, resolved_group)
        next_state = flow_child_group_parent_state(parent, resolved_group)

        next =
          parent
          |> Map.merge(%{
            state: next_state,
            version: Map.fetch!(parent, :version) + 1,
            updated_at_ms: now_ms,
            child_groups: updated_groups
          })
          |> flow_clear_parent_if_resolved(resolved_group)
          |> flow_stamp_terminal_retention(now_ms)

        {:ok, next}
    end
  end

  defp flow_find_open_child_group(groups, child_id) do
    Enum.find(groups, fn {_group_id, group} ->
      is_nil(Map.get(group, "resolved")) and
        Map.get(group, "children", %{})[child_id] == "running"
    end)
  end

  defp flow_child_group_count_terminal(group, child, status) do
    child_id = Map.fetch!(child, :id)
    summary_key = status
    result = flow_child_terminal_result(child, status)

    group
    |> update_in(["children"], &Map.put(&1, child_id, status))
    |> update_in(["results"], &Map.put(&1 || %{}, child_id, result))
    |> update_in(["summary", summary_key], fn count -> (count || 0) + 1 end)
  end

  defp flow_child_terminal_result(child, status) do
    %{"status" => status}
    |> maybe_put_group_result_ref("result_ref", Map.get(child, :result_ref))
    |> maybe_put_group_result_ref("error_ref", Map.get(child, :error_ref))
  end

  defp maybe_put_group_result_ref(result, _key, nil), do: result
  defp maybe_put_group_result_ref(result, key, value), do: Map.put(result, key, value)

  defp flow_child_group_resolve(%{"wait" => "any"} = group, "completed") do
    Map.put(group, "resolved", "success")
  end

  defp flow_child_group_resolve(%{"wait" => "any"} = group, status)
       when status in ["failed", "cancelled"] do
    if Map.get(group, "on_child_failed") == "fail_parent" do
      Map.put(group, "resolved", "failure")
    else
      flow_child_group_resolve_any_terminal(group)
    end
  end

  defp flow_child_group_resolve(group, status) when status in ["failed", "cancelled"] do
    if Map.get(group, "on_child_failed") == "fail_parent" do
      Map.put(group, "resolved", "failure")
    else
      flow_child_group_resolve_all_terminal(group)
    end
  end

  defp flow_child_group_resolve(group, _status), do: flow_child_group_resolve_all_terminal(group)

  defp flow_child_group_resolve_any_terminal(group) do
    summary = Map.get(group, "summary", %{})
    total = Map.get(summary, "total", 0)
    completed = Map.get(summary, "completed", 0)

    terminal_count =
      completed + Map.get(summary, "failed", 0) + Map.get(summary, "cancelled", 0)

    cond do
      completed > 0 -> Map.put(group, "resolved", "success")
      terminal_count >= total -> Map.put(group, "resolved", "failure")
      true -> group
    end
  end

  defp flow_child_group_resolve_all_terminal(group) do
    summary = Map.get(group, "summary", %{})
    total = Map.get(summary, "total", 0)

    terminal_count =
      Map.get(summary, "completed", 0) + Map.get(summary, "failed", 0) +
        Map.get(summary, "cancelled", 0)

    if terminal_count >= total do
      Map.put(group, "resolved", "success")
    else
      group
    end
  end

  defp flow_child_group_parent_state(_parent, %{
         "resolved" => resolved,
         "exhaust_to" => exhaust_to
       })
       when resolved in ["success", "failure"] and is_map(exhaust_to) do
    Map.fetch!(exhaust_to, resolved)
  end

  defp flow_child_group_parent_state(parent, _group), do: Map.fetch!(parent, :state)

  defp flow_clear_parent_if_resolved(next, %{"resolved" => resolved})
       when resolved in ["success", "failure"] do
    next
    |> Map.put(:next_run_at_ms, nil)
    |> Map.put(:ttl_ms, nil)
    |> Map.put(:lease_owner, nil)
    |> Map.put(:lease_token, nil)
    |> Map.put(:lease_deadline_ms, 0)
  end

  defp flow_clear_parent_if_resolved(next, _group), do: next

  defp flow_maybe_apply_resolved_parent_terminal(state, parent, now_ms) do
    case Map.get(parent, :state) do
      "completed" -> flow_maybe_apply_child_terminal(state, parent, "completed", now_ms)
      "failed" -> flow_maybe_apply_child_terminal(state, parent, "failed", now_ms)
      "cancelled" -> flow_maybe_apply_child_terminal(state, parent, "cancelled", now_ms)
      _state -> :ok
    end
  end

  defp flow_cancel_refresh_attrs(attrs) do
    if Map.has_key?(attrs, :error) do
      attrs
    else
      Map.put(attrs, :error, true)
    end
  end

  defp flow_cancel_many_prepare(state, attrs_list) do
    existing_records = flow_read_records(state, attrs_list)

    attrs_list
    |> Enum.zip(existing_records)
    |> Enum.reduce_while({:ok, [], false, false}, fn
      {%{id: _id} = attrs, existing}, {:ok, acc, has_values?, has_after_terminal?} ->
        now_ms = flow_attrs_now_ms(attrs)

        case existing do
          nil ->
            {:halt, {:error, "ERR flow not found"}}

          record ->
            case flow_prepare_cancel_existing_record(record, attrs, now_ms) do
              {:ok, record, next} ->
                {:cont,
                 {:ok, [{record, next, attrs} | acc],
                  has_values? or flow_attrs_have_record_values?(attrs),
                  has_after_terminal? or flow_terminal_after_required?(:cancel, next)}}

              {:error, _reason} = error ->
                {:halt, error}
            end
        end

      {_bad, _existing}, {:ok, _acc, _has_values?, _has_after_terminal?} ->
        {:halt, {:error, "ERR flow id must be a non-empty string"}}
    end)
    |> case do
      {:ok, plans, has_record_values?, has_after_terminal?} ->
        {:ok, Enum.reverse(plans), has_record_values?, has_after_terminal?}

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_cancel_many_apply(state, plans, has_record_values?, has_after_terminal?) do
    with :ok <- flow_cancel_many_put_record_values(state, plans, has_record_values?),
         :ok <- flow_terminal_transition_move_indexes(state, plans),
         :ok <- flow_claim_put_state_records(state, plans),
         :ok <- flow_many_put_history(state, plans, "cancelled"),
         :ok <- flow_many_after_terminal(state, plans, "cancelled", has_after_terminal?) do
      :ok
    end
  end

  defp flow_cancel_many_put_record_values(_state, _plans, false), do: :ok

  defp flow_cancel_many_put_record_values(state, plans, :unknown) do
    if flow_many_plans_have_record_values?(plans) do
      flow_cancel_many_put_record_values(state, plans, true)
    else
      :ok
    end
  end

  defp flow_cancel_many_put_record_values(state, plans, true) do
    Enum.reduce_while(plans, :ok, fn {_record, next, attrs}, :ok ->
      refresh_attrs = flow_cancel_refresh_attrs(attrs)

      case do_flow_put_record_values(state, next, attrs) do
        :ok ->
          :ok = flow_refresh_terminal_value_expirations(state, next, refresh_attrs)
          {:cont, :ok}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp do_flow_retention_cleanup(state, attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    limit = Map.get(attrs, :limit, 100)
    ets_entries = flow_retention_expired_state_entries(state, now_ms, limit)

    ets_result =
      Enum.reduce_while(ets_entries, {:ok, %{flows: 0, history: 0, values: 0}}, fn entry,
                                                                                   {:ok, acc} ->
        case flow_retention_cleanup_entry(state, entry) do
          {:ok, counts} -> {:cont, {:ok, flow_retention_merge_counts(acc, counts)}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    with {:ok, acc} <- ets_result do
      seen =
        ets_entries
        |> Enum.map(fn {state_key, _value, _expire_at_ms, _fid, _offset, _value_size} ->
          state_key
        end)
        |> MapSet.new()

      state
      |> flow_retention_expired_lmdb_state_keys(now_ms, max(limit - MapSet.size(seen), 0), seen)
      |> Enum.reduce_while({:ok, acc}, fn state_key, {:ok, acc} ->
        case flow_retention_cleanup_lmdb_state_key(state, state_key) do
          {:ok, counts} -> {:cont, {:ok, flow_retention_merge_counts(acc, counts)}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  defp flow_retention_expired_lmdb_state_keys(_state, _now_ms, remaining, _seen)
       when remaining <= 0,
       do: []

  defp flow_retention_expired_lmdb_state_keys(state, now_ms, remaining, seen) do
    if flow_lmdb_mirror?(state) do
      case Ferricstore.Flow.LMDB.expired_terminal_state_keys(
             flow_lmdb_record_path(state),
             now_ms,
             remaining
           ) do
        {:ok, state_keys} ->
          Enum.reject(state_keys, &MapSet.member?(seen, &1))

        {:error, _reason} ->
          []
      end
    else
      []
    end
  end

  defp flow_retention_cleanup_lmdb_state_key(state, state_key) do
    case flow_retention_decode_lmdb_state_record(state, state_key) do
      {:ok, record} ->
        if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
          flow_retention_cleanup_record(state, state_key, record)
        else
          {:ok, %{flows: 0, history: 0, values: 0}}
        end

      :miss ->
        {:ok, %{flows: 0, history: 0, values: 0}}
    end
  end

  defp flow_retention_decode_lmdb_state_record(state, state_key) do
    case Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), state_key) do
      {:ok, blob} ->
        case Ferricstore.Flow.LMDB.decode_value(blob, 0) do
          {:ok, value} -> flow_decode_record_blob(value)
          _ -> :miss
        end

      _ ->
        :miss
    end
  end

  defp flow_retention_expired_state_entries(state, now_ms, limit) do
    prefix = "f:{f"
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :"$2", :"$3", :_, :"$5", :"$6", :"$7"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:andalso, {:==, {:binary_part, :"$1", 0, prefix_len}, prefix},
            {:andalso, {:>, :"$3", 0}, {:<, :"$3", now_ms + 1}}}}}
       ], [{{:"$1", :"$2", :"$3", :"$5", :"$6", :"$7"}}]}
    ]

    state.ets
    |> :ets.select(match_spec)
    |> Enum.filter(fn {key, _value, _expire_at_ms, _fid, _offset, _value_size} ->
      FlowKeys.state_key?(key)
    end)
    |> Enum.take(limit)
  end

  defp flow_retention_cleanup_entry(
         state,
         {state_key, value, _expire_at_ms, fid, offset, value_size}
       ) do
    case flow_retention_decode_state_record(state, state_key, value, fid, offset, value_size) do
      {:ok, record} ->
        if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
          flow_retention_cleanup_record(state, state_key, record)
        else
          {:ok, %{flows: 0, history: 0, values: 0}}
        end

      :miss ->
        {:ok, %{flows: 0, history: 0, values: 0}}
    end
  end

  defp flow_retention_decode_state_record(_state, _key, value, _fid, _offset, _value_size)
       when is_binary(value) do
    flow_decode_record_blob(value)
  end

  defp flow_retention_decode_state_record(state, key, nil, fid, offset, value_size)
       when valid_cold_location(fid, offset, value_size) do
    case ColdRead.pread_at(sm_file_path(state, fid), offset, key, @cold_read_timeout_ms) do
      {:ok, value} ->
        case materialize_cold_blob_value(state, value) do
          {:ok, materialized} -> flow_decode_record_blob(materialized)
          {:error, _reason} -> :miss
        end

      {:error, _reason} ->
        :miss
    end
  end

  defp flow_retention_decode_state_record(_state, _key, _value, _fid, _offset, _value_size),
    do: :miss

  defp flow_retention_cleanup_record(state, state_key, record) do
    history_key = FlowKeys.history_key(Map.fetch!(record, :id), Map.get(record, :partition_key))
    history_entries = flow_retention_history_entries(state, history_key)
    history_keys = Enum.map(history_entries, fn {key, _event_id} -> key end)
    history_values = sm_store_batch_get(state, history_keys, &sm_file_path/2)

    value_refs =
      record
      |> flow_retention_record_value_refs()
      |> Kernel.++(flow_retention_history_value_refs(history_values))
      |> Enum.uniq()

    shared_value_links = flow_retention_shared_value_links(state, record)
    shared_link_keys = Enum.map(shared_value_links, fn {key, _ref} -> key end)
    shared_value_refs = Enum.map(shared_value_links, fn {_key, ref} -> ref end)

    with :ok <- flow_retention_delete_history_index(state, history_key, history_entries),
         {:ok, history_count} <- flow_retention_delete_keys(state, history_keys),
         {:ok, values_count} <- flow_retention_delete_keys(state, value_refs),
         {:ok, shared_values_count} <- flow_retention_delete_keys(state, shared_value_refs),
         {:ok, _shared_link_count} <- flow_retention_delete_keys(state, shared_link_keys),
         :ok <- do_delete(state, state_key) do
      {:ok, %{flows: 1, history: history_count, values: values_count + shared_values_count}}
    end
  end

  defp flow_retention_shared_value_links(state, record) do
    prefix =
      FlowKeys.shared_value_link_prefix(
        Map.fetch!(record, :id),
        Map.get(record, :partition_key)
      )

    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    keys = :ets.select(state.ets, match_spec)
    values = sm_store_batch_get(state, keys, &sm_file_path/2)

    keys
    |> Enum.zip(values)
    |> Enum.flat_map(fn
      {key, ref} when is_binary(ref) and ref != "" -> [{key, ref}]
      _other -> []
    end)
  end

  defp flow_retention_history_entries(state, history_key) do
    prefix = "X:" <> history_key <> <<0>>
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    ets_entries =
      state.ets
      |> :ets.select(match_spec)
      |> Enum.map(fn key -> {key, binary_part(key, prefix_len, byte_size(key) - prefix_len)} end)

    lmdb_entries =
      if flow_lmdb_mirror?(state) do
        case Ferricstore.Flow.LMDB.history_compound_entries(
               flow_lmdb_record_path(state),
               history_key,
               flow_retention_history_lmdb_scan_limit()
             ) do
          {:ok, entries} -> entries
          {:error, _reason} -> []
        end
      else
        []
      end

    (ets_entries ++ lmdb_entries)
    |> Enum.uniq_by(fn {key, _event_id} -> key end)
  end

  defp flow_retention_delete_history_index(_state, _history_key, []), do: :ok

  defp flow_retention_delete_history_index(state, history_key, entries) do
    event_ids = Enum.map(entries, fn {_key, event_id} -> event_id end)

    with :ok <- flow_index_delete_members(state, history_key, event_ids) do
      if flow_lmdb_mirror?(state) do
        with_lmdb_mirror_shard(state, fn ->
          Enum.each(entries, fn {_key, event_id} ->
            queue_lmdb_history_index_delete(nil, history_key, event_id, flow_event_ms(event_id))
          end)
        end)
      end

      :ok
    end
  end

  defp flow_retention_history_lmdb_scan_limit do
    :ferricstore
    |> Application.get_env(:flow_lmdb_history_cleanup_scan_limit, 100_000)
    |> flow_retention_positive_integer(100_000)
  end

  defp flow_retention_positive_integer(value, _default) when is_integer(value) and value > 0,
    do: value

  defp flow_retention_positive_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed > 0 -> parsed
      _ -> default
    end
  end

  defp flow_retention_positive_integer(_value, default), do: default

  defp flow_event_ms(event_id) when is_binary(event_id) do
    event_id
    |> String.split("-", parts: 2)
    |> case do
      [ms, _seq] -> String.to_integer(ms)
      _ -> 0
    end
  rescue
    _ -> 0
  end

  defp flow_event_ms(_event_id), do: 0

  defp flow_retention_record_value_refs(record) do
    record_refs =
      [:payload_ref, :result_ref, :error_ref]
      |> Enum.flat_map(fn key ->
        string_key = Atom.to_string(key)
        [Map.get(record, key), Map.get(record, string_key)]
      end)

    named_refs =
      record
      |> flow_retention_named_value_refs()
      |> Map.values()
      |> Enum.map(&Map.get(&1, :ref))

    (record_refs ++ named_refs)
    |> Enum.filter(&flow_owned_value_ref?/1)
  end

  defp flow_retention_named_value_refs(%{} = record) do
    cond do
      Map.has_key?(record, :value_refs) ->
        flow_record_value_refs(record)

      refs = Map.get(record, "value_refs") ->
        flow_normalize_value_refs(refs)

      true ->
        %{}
    end
  end

  defp flow_retention_named_value_refs(_record), do: %{}

  defp flow_retention_history_value_refs(values) do
    values
    |> Enum.flat_map(fn
      value when is_binary(value) ->
        value
        |> Flow.decode_history_fields()
        |> flow_history_fields_to_map()
        |> flow_retention_record_value_refs()

      _other ->
        []
    end)
    |> Enum.filter(&flow_owned_value_ref?/1)
  end

  defp flow_owned_value_ref?(ref) when is_binary(ref) do
    String.starts_with?(ref, "f:{f") and String.contains?(ref, "}:v:")
  end

  defp flow_owned_value_ref?(_ref), do: false

  defp flow_retention_delete_keys(state, keys) do
    keys
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, 0}, fn key, {:ok, count} ->
      case do_delete(state, key) do
        :ok -> {:cont, {:ok, count + 1}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_retention_merge_counts(left, right) do
    %{
      flows: Map.get(left, :flows, 0) + Map.get(right, :flows, 0),
      history: Map.get(left, :history, 0) + Map.get(right, :history, 0),
      values: Map.get(left, :values, 0) + Map.get(right, :values, 0)
    }
  end

  defp do_flow_rewind(state, %{id: id, to_event: to_event} = attrs) do
    now_ms = flow_attrs_now_ms(attrs)
    partition_key = Map.get(attrs, :partition_key)

    with {:ok, record} <- flow_require_record(state, id, partition_key),
         :ok <- flow_require_rewindable(record),
         :ok <- flow_require_expected_state(record, Map.get(attrs, :expect_state)),
         {:ok, target_fields} <- flow_history_event_fields(state, id, to_event, partition_key),
         {:ok, next} <- flow_rewind_record(record, target_fields, attrs, now_ms) do
      next = Map.put(next, :rewound_to_event_id, to_event)

      with :ok <- flow_validate_record_keys(record),
           :ok <- flow_validate_record_keys(next),
           :ok <- flow_transition_move_indexes(state, [{record, next}]),
           :ok <- flow_refresh_record_value_expirations(state, next, %{}),
           :ok <-
             flow_put_state_record(
               state,
               FlowKeys.state_key(id, partition_key),
               next
             ),
           :ok <- flow_history_put_planned(state, record, next, "rewound", now_ms),
           :ok <- flow_after_history_put(state, next) do
        :ok
      end
    end
  end

  defp flow_prepare_claim_candidate_record(
         record,
         _id,
         type,
         state_filter,
         worker,
         lease_ms,
         now_ms,
         due_score
       ) do
    case record do
      nil ->
        :delete_due

      %{type: record_type} when record_type != type ->
        {:skip, flow_claim_restore_due_score(record, due_score)}

      %{state: record_state} = record ->
        cond do
          flow_claim_state_excluded?(state_filter, record_state) ->
            {:skip, flow_claim_restore_due_score(record, due_score)}

          not flow_claim_record_due_ready?(record, now_ms) ->
            {:skip, flow_claim_restore_due_score(record, due_score)}

          flow_claim_state_match?(state_filter, record_state) ->
            next_version = Map.fetch!(record, :version) + 1
            next_fencing_token = Map.get(record, :fencing_token, 0) + 1
            deadline_ms = now_ms + lease_ms

            token =
              worker <>
                ":" <> Integer.to_string(now_ms) <> ":" <> Integer.to_string(next_fencing_token)

            next =
              flow_claim_next_record(
                record,
                next_version,
                next_fencing_token,
                worker,
                token,
                deadline_ms,
                now_ms
              )

            with {:ok, from_due_score} <- flow_claim_numeric_score(due_score),
                 :ok <- flow_validate_claim_next_record_keys(next) do
              {:ok, record, next, from_due_score}
            else
              _ -> {:skip, flow_claim_restore_due_score(record, due_score)}
            end

          true ->
            :delete_due
        end

      _record ->
        :delete_due
    end
  end

  defp flow_claim_next_record(
         %{
           state: _state,
           version: _version,
           fencing_token: _fencing_token,
           updated_at_ms: _updated_at_ms,
           ttl_ms: _ttl_ms,
           retention_ttl_ms: _retention_ttl_ms,
           terminal_retention_until_ms: _terminal_retention_until_ms,
           history_hot_max_events: _history_hot_max_events,
           history_max_events: _history_max_events,
           lease_owner: _lease_owner,
           lease_token: _lease_token,
           lease_deadline_ms: _lease_deadline_ms,
           next_run_at_ms: _next_run_at_ms,
           run_state: _run_state
         } = record,
         next_version,
         next_fencing_token,
         worker,
         token,
         deadline_ms,
         now_ms
       ) do
    %{
      record
      | state: "running",
        version: next_version,
        fencing_token: next_fencing_token,
        updated_at_ms: now_ms,
        ttl_ms: nil,
        terminal_retention_until_ms: nil,
        lease_owner: worker,
        lease_token: token,
        lease_deadline_ms: deadline_ms,
        next_run_at_ms: deadline_ms,
        run_state: flow_claim_run_state(record)
    }
  end

  defp flow_claim_next_record(
         record,
         next_version,
         next_fencing_token,
         worker,
         token,
         deadline_ms,
         now_ms
       ) do
    Map.merge(record, %{
      state: "running",
      version: next_version,
      fencing_token: next_fencing_token,
      updated_at_ms: now_ms,
      ttl_ms: nil,
      retention_ttl_ms: Map.get(record, :retention_ttl_ms),
      terminal_retention_until_ms: nil,
      history_hot_max_events: Map.get(record, :history_hot_max_events),
      history_max_events: Map.get(record, :history_max_events),
      lease_owner: worker,
      lease_token: token,
      lease_deadline_ms: deadline_ms,
      next_run_at_ms: deadline_ms,
      run_state: flow_claim_run_state(record)
    })
  end

  defp flow_claim_state_excluded?({:exclude, _state_filter, exclude_states}, state),
    do: state in exclude_states

  defp flow_claim_state_excluded?(_state_filter, _state), do: false

  defp flow_claim_state_match?({:exclude, state_filter, _exclude_states}, state),
    do: flow_claim_state_match?(state_filter, state)

  defp flow_claim_state_match?(:any, state) when is_binary(state), do: true
  defp flow_claim_state_match?(states, state) when is_list(states), do: state in states
  defp flow_claim_state_match?(state, state), do: true
  defp flow_claim_state_match?(_state_filter, _state), do: false

  defp flow_claim_run_state(%{state: "running"} = record),
    do: Map.get(record, :run_state) || "queued"

  defp flow_claim_run_state(%{state: flow_state}), do: flow_state

  defp flow_claim_plan_pair(
         {:native_claim, next, _entry, _state_key, _value, _previous_history_ms}
       ),
       do: {next, next}

  defp flow_claim_plan_pair({record, next, _from_due_score}), do: {record, next}
  defp flow_claim_plan_pair({record, next, _history_meta, _attrs}), do: {record, next}
  defp flow_claim_plan_pair({record, next}), do: {record, next}

  defp flow_claim_plan_due_score(
         {:native_claim, _next,
          {_id, _from_due_key, due_score, _to_due_key, _to_due_score, _from_state_key,
           _from_state_score, _to_state_key, _to_state_score, _inflight_key, _worker_key,
           _lease_score}, _state_key, _value, _previous_history_ms}
       ),
       do: flow_claim_numeric_score(due_score)

  defp flow_claim_plan_due_score({_record, _next, due_score}),
    do: flow_claim_numeric_score(due_score)

  defp flow_claim_plan_due_score({record, _next}),
    do: flow_claim_numeric_score(Map.get(record, :next_run_at_ms))

  defp flow_claim_record_state_score(record),
    do: flow_claim_numeric_score(Map.get(record, :updated_at_ms, 0))

  defp flow_claim_record_due_ready?(record, now_ms) do
    with {:ok, due_score} <- flow_claim_numeric_score(Map.get(record, :next_run_at_ms)),
         {:ok, now_score} <- flow_claim_numeric_score(now_ms) do
      due_score <= now_score
    else
      _ -> false
    end
  end

  defp flow_claim_numeric_score(score) when is_float(score), do: {:ok, score}
  defp flow_claim_numeric_score(score) when is_integer(score), do: {:ok, score * 1.0}
  defp flow_claim_numeric_score(_score), do: :error

  defp flow_claim_restore_due_score(record, due_score) do
    case flow_claim_numeric_score(Map.get(record, :next_run_at_ms)) do
      {:ok, score} ->
        score

      :error ->
        case flow_claim_numeric_score(due_score) do
          {:ok, score} -> score
          :error -> 0.0
        end
    end
  end

  defp flow_apply_claim_batch(_state, _due_key, [], [], _now_ms), do: :ok

  defp flow_apply_claim_batch(state, due_key, plans, stale_due_ids, now_ms) do
    phase_meta =
      state
      |> flow_claim_due_phase_meta()
      |> Map.merge(%{plans: length(plans), stale_due_ids: length(stale_due_ids)})

    with :ok <-
           flow_claim_due_phase(:delete_stale_due, phase_meta, fn ->
             flow_zset_delete_members_from_key(state, due_key, stale_due_ids)
           end),
         :ok <-
           flow_claim_due_phase(:move_indexes, phase_meta, fn ->
             flow_claim_move_indexes(state, plans)
           end),
         :ok <-
           flow_claim_due_phase(:state_write, phase_meta, fn ->
             flow_claim_put_state_records(state, plans)
           end),
         :ok <-
           flow_claim_due_phase(:history_write, phase_meta, fn ->
             flow_claim_put_history(state, plans, now_ms)
           end) do
      :ok
    end
  end

  defp flow_claim_move_indexes(_state, []), do: :ok

  defp flow_claim_move_indexes(state, plans) do
    case flow_claim_move_indexes_fast(state, plans) do
      :ok -> flow_claim_move_due_any_indexes(state, plans)
      :fallback -> flow_claim_move_indexes_generic(state, plans)
    end
  end

  defp flow_claim_move_indexes_generic(state, plans) do
    {moves, deletes, puts} =
      Enum.reduce(plans, {[], [], %{}}, fn plan, {moves, deletes, puts} ->
        {record, next} = flow_claim_plan_pair(plan)

        {moves, deletes, puts} =
          flow_claim_due_index_plan(record, next, moves, deletes, puts)

        moves =
          record
          |> flow_claim_state_index_move(next)
          |> then(&[&1 | moves])

        moves =
          record
          |> flow_claim_metadata_index_moves(next)
          |> Enum.reduce(moves, fn move, acc -> [move | acc] end)

        flow_claim_queue_old_terminal_lmdb_deletes(state, record)
        deletes = flow_claim_old_running_index_deletes(record, deletes)
        puts = flow_claim_new_running_index_puts(next, puts)

        {moves, deletes, puts}
      end)

    with :ok <- flow_index_move_entries(state, Enum.reverse(moves)),
         :ok <- flow_zset_index_delete_grouped(state, deletes) do
      flow_claim_put_grouped_zset_entries(state, puts)
    end
  end

  defp flow_claim_move_due_any_indexes(_state, []), do: :ok

  defp flow_claim_move_due_any_indexes(state, plans) do
    if flow_due_any_index_enabled?() do
      {moves, deletes, puts} =
        Enum.reduce(plans, {[], [], %{}}, fn plan, acc ->
          {record, next} = flow_claim_plan_pair(plan)
          flow_due_any_index_plan(record, next, acc)
        end)

      with :ok <- flow_index_move_lifecycle_entries(state, Enum.reverse(moves)),
           :ok <- flow_zset_lifecycle_index_delete_grouped(state, deletes) do
        flow_claim_put_grouped_zset_entries(state, puts)
      end
    else
      :ok
    end
  end

  defp flow_claim_move_indexes_fast(state, plans) do
    cond do
      not flow_index_tables?(state) ->
        :ok

      flow_native_index(state) == nil ->
        :fallback

      true ->
        phase_meta =
          state
          |> flow_claim_due_phase_meta()
          |> Map.merge(%{plans: length(plans)})

        case flow_claim_due_internal_phase(
               :fast_index_entries,
               phase_meta,
               %{items: length(plans)},
               fn -> flow_claim_fast_index_entries(state, plans) end
             ) do
          {:ok, entries} ->
            flow_claim_apply_fast_index_entries(state, entries)

          :fallback ->
            :fallback
        end
    end
  end

  defp flow_claim_fast_index_entries(
         _state,
         [{:native_claim, _next, _entry, _key, _value, _prev} | _] = plans
       ) do
    Enum.reduce_while(plans, {:ok, []}, fn
      {:native_claim, _next, entry, _state_key, _value, _previous_history_ms}, {:ok, acc} ->
        {:cont, {:ok, [entry | acc]}}

      _other, _acc ->
        {:halt, :fallback}
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      :fallback -> :fallback
    end
  end

  defp flow_claim_fast_index_entries(_state, plans) do
    Enum.reduce_while(plans, {:ok, [], nil, nil, nil, nil, nil, nil}, fn plan,
                                                                         {:ok, acc,
                                                                          from_due_cache,
                                                                          to_due_cache,
                                                                          from_state_cache,
                                                                          to_state_cache,
                                                                          inflight_cache,
                                                                          worker_cache} ->
      {record, next} = flow_claim_plan_pair(plan)

      if flow_claim_fast_index_record_shape?(record, next) do
        id = next.id
        record_partition_key = Map.get(record, :partition_key)
        partition_key = Map.get(next, :partition_key)

        {from_due_key, from_due_cache} =
          flow_claim_cached_due_index_key(from_due_cache, record)

        {to_due_key, to_due_cache} =
          flow_claim_cached_due_index_key(to_due_cache, next)

        {from_state_key, from_state_cache} =
          flow_claim_cached_state_index_key(
            from_state_cache,
            record.type,
            record.state,
            record_partition_key
          )

        {to_state_key, to_state_cache} =
          flow_claim_cached_state_index_key(to_state_cache, next.type, next.state, partition_key)

        {inflight_key, inflight_cache} =
          flow_claim_cached_inflight_index_key(inflight_cache, next.type, partition_key)

        {worker_key, worker_cache} =
          flow_claim_cached_worker_index_key(
            worker_cache,
            Map.get(next, :lease_owner, ""),
            partition_key
          )

        with true <- is_binary(from_due_key) and is_binary(to_due_key),
             {:ok, from_due_score} <- flow_claim_plan_due_score(plan),
             {:ok, from_state_score} <- flow_claim_record_state_score(record) do
          lease_score = Map.get(next, :lease_deadline_ms, 0) * 1.0

          entry =
            {id, from_due_key, from_due_score * 1.0, to_due_key,
             Map.fetch!(next, :next_run_at_ms) * 1.0, from_state_key, from_state_score * 1.0,
             to_state_key, Map.get(next, :updated_at_ms, 0) * 1.0, inflight_key, worker_key,
             lease_score}

          {:cont,
           {:ok, [entry | acc], from_due_cache, to_due_cache, from_state_cache, to_state_cache,
            inflight_cache, worker_cache}}
        else
          _ -> {:halt, :fallback}
        end
      else
        {:halt, :fallback}
      end
    end)
    |> case do
      {:ok, entries, _from_due_cache, _to_due_cache, _from_state_cache, _to_state_cache,
       _inflight_cache, _worker_cache} ->
        {:ok, entries}

      :fallback ->
        :fallback
    end
  end

  defp flow_claim_cached_due_index_key(cache, %{next_run_at_ms: nil}), do: {nil, cache}

  defp flow_claim_cached_due_index_key(
         cache,
         %{type: type, state: flow_state, priority: priority} = record
       ) do
    partition_key = Map.get(record, :partition_key)
    cache_key = {type, flow_state, priority, partition_key}

    case cache do
      {^cache_key, key} ->
        {key, cache}

      _ ->
        key = FlowKeys.due_key(type, flow_state, priority, partition_key)
        {key, {cache_key, key}}
    end
  end

  defp flow_claim_cached_state_index_key(cache, type, flow_state, partition_key) do
    cache_key = {type, flow_state, partition_key}

    case cache do
      {^cache_key, key} ->
        {key, cache}

      _ ->
        key = FlowKeys.state_index_key(type, flow_state, partition_key)
        {key, {cache_key, key}}
    end
  end

  defp flow_claim_cached_inflight_index_key(cache, type, partition_key) do
    cache_key = {type, partition_key}

    case cache do
      {^cache_key, key} ->
        {key, cache}

      _ ->
        key = FlowKeys.inflight_index_key(type, partition_key)
        {key, {cache_key, key}}
    end
  end

  defp flow_claim_cached_worker_index_key(cache, worker, partition_key) do
    cache_key = {worker, partition_key}

    case cache do
      {^cache_key, key} ->
        {key, cache}

      _ ->
        key = FlowKeys.worker_index_key(worker, partition_key)
        {key, {cache_key, key}}
    end
  end

  defp flow_claim_fast_index_record_shape?(record, next) do
    Map.get(record, :state) != "running" and Map.get(next, :state) == "running" and
      flow_claim_fast_metadata_empty?(record) and flow_claim_fast_metadata_empty?(next)
  end

  defp flow_claim_fast_metadata_empty?(record) do
    id = Map.get(record, :id)

    flow_blank_metadata?(Map.get(record, :parent_flow_id)) and
      flow_blank_metadata?(Map.get(record, :correlation_id)) and
      Map.get(record, :root_flow_id) in [nil, "", id]
  end

  defp flow_claim_apply_fast_index_entries(state, entries) do
    phase_meta =
      state
      |> flow_claim_due_phase_meta()
      |> Map.merge(%{entries: length(entries)})

    case flow_native_index(state) do
      nil ->
        flow_claim_apply_fast_index_entries_ets(state, entries, phase_meta)

      _native ->
        flow_claim_due_internal_phase(
          :fast_index_track_originals,
          phase_meta,
          %{items: length(entries)},
          fn ->
            flow_claim_queue_fast_index_rollback(state, entries)
          end
        )

        flow_claim_due_internal_phase(
          :fast_index_native_due_apply,
          phase_meta,
          %{items: length(entries)},
          fn ->
            flow_native_apply_claim_entries(state, entries)
          end
        )
    end
  end

  defp flow_claim_apply_fast_index_entries_ets(state, entries, phase_meta) do
    flow_claim_due_internal_phase(
      :fast_index_track_originals,
      phase_meta,
      %{items: length(entries)},
      fn ->
        flow_claim_track_fast_index_originals(state, entries)
      end
    )

    {index_deletes, lookup_deletes, index_inserts, lookup_inserts, count_deltas} =
      flow_claim_due_internal_phase(
        :fast_index_build_ops,
        phase_meta,
        %{items: length(entries)},
        fn ->
          Enum.reduce(entries, {[], [], [], [], %{}}, fn {id, _from_due_key, _from_due_score,
                                                          _to_due_key, _to_due_score,
                                                          from_state_key, from_state_score,
                                                          to_state_key, to_state_score,
                                                          inflight_key, worker_key, lease_score},
                                                         {index_deletes, lookup_deletes,
                                                          index_inserts, lookup_inserts,
                                                          count_deltas} ->
            # Due scheduling is native-authoritative on the claim hot path.
            # Keep ETS state/running indexes synchronous for existing APIs, but
            # do not synchronously project due deletes/inserts here; stale ETS
            # due rows are ignored by claim_due because reads come from native.
            index_deletes = [{from_state_key, from_state_score, id} | index_deletes]

            lookup_deletes = [{from_state_key, id} | lookup_deletes]

            index_inserts = [
              {{to_state_key, to_state_score, id}, true},
              {{inflight_key, lease_score, id}, true},
              {{worker_key, lease_score, id}, true}
              | index_inserts
            ]

            lookup_inserts = [
              {{to_state_key, id}, to_state_score},
              {{inflight_key, id}, lease_score},
              {{worker_key, id}, lease_score}
              | lookup_inserts
            ]

            count_deltas =
              count_deltas
              |> Map.update(from_state_key, -1, &(&1 - 1))
              |> Map.update(to_state_key, 1, &(&1 + 1))
              |> Map.update(inflight_key, 1, &(&1 + 1))
              |> Map.update(worker_key, 1, &(&1 + 1))

            {index_deletes, lookup_deletes, index_inserts, lookup_inserts, count_deltas}
          end)
        end
      )

    flow_claim_due_internal_phase(
      :fast_index_ets_index_deletes,
      phase_meta,
      %{items: length(index_deletes)},
      fn ->
        Enum.each(index_deletes, &:ets.delete(state.flow_index_name, &1))
      end
    )

    flow_claim_due_internal_phase(
      :fast_index_ets_lookup_deletes,
      phase_meta,
      %{items: length(lookup_deletes)},
      fn ->
        Enum.each(lookup_deletes, &:ets.delete(state.flow_lookup_name, &1))
      end
    )

    flow_claim_due_internal_phase(
      :fast_index_ets_index_inserts,
      phase_meta,
      %{items: length(index_inserts)},
      fn ->
        if index_inserts != [], do: :ets.insert(state.flow_index_name, index_inserts)
      end
    )

    flow_claim_due_internal_phase(
      :fast_index_ets_lookup_inserts,
      phase_meta,
      %{items: length(lookup_inserts)},
      fn ->
        if lookup_inserts != [], do: :ets.insert(state.flow_lookup_name, lookup_inserts)
      end
    )

    flow_claim_due_internal_phase(
      :fast_index_native_due_apply,
      phase_meta,
      %{items: length(entries)},
      fn ->
        flow_native_apply_claim_entries(state, entries)
      end
    )

    flow_claim_due_internal_phase(
      :fast_index_count_updates,
      phase_meta,
      %{items: map_size(count_deltas)},
      fn ->
        Enum.each(count_deltas, fn
          {_key, 0} ->
            :ok

          {key, delta} ->
            :ets.update_counter(
              state.flow_lookup_name,
              {:count, key},
              {2, delta},
              {{:count, key}, 0}
            )
        end)
      end
    )

    :ok
  end

  defp flow_claim_track_fast_index_originals(state, entries) do
    flow_claim_queue_fast_index_rollback(state, entries)
    flow_claim_track_fast_index_count_originals(state, entries)
  end

  defp flow_claim_queue_fast_index_rollback(state, entries) do
    case Process.get(:sm_pending_fast_claim_index_rollbacks, :undefined) do
      rollbacks when is_list(rollbacks) ->
        rollback = {state.flow_index_name, state.flow_lookup_name, entries}
        Process.put(:sm_pending_fast_claim_index_rollbacks, [rollback | rollbacks])

      _ ->
        :ok
    end
  end

  defp flow_claim_track_fast_index_count_originals(state, entries) do
    keys =
      entries
      |> Enum.flat_map(fn {_id, _from_due_key, _from_due_score, _to_due_key, _to_due_score,
                           from_state_key, _from_state_score, to_state_key, _to_state_score,
                           inflight_key, worker_key, _lease_score} ->
        [from_state_key, to_state_key, inflight_key, worker_key]
      end)
      |> Enum.uniq()

    Enum.each(keys, &track_flow_index_count_original(state, &1))
  end

  defp flow_claim_queue_old_terminal_lmdb_deletes(state, record) do
    maybe_queue_terminal_lmdb_index_delete(state, record)

    if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      queue_lmdb_metadata_index_deletes(state, record)
    end

    :ok
  end

  defp flow_claim_due_index_plan(record, next, moves, deletes, puts) do
    {moves, deletes, puts} = flow_due_state_index_plan(record, next, {moves, deletes, puts})

    if flow_due_any_index_enabled?() do
      flow_due_any_index_plan(record, next, {moves, deletes, puts})
    else
      {moves, deletes, puts}
    end
  end

  defp flow_due_state_index_plan(record, next, {moves, deletes, puts}) do
    flow_due_index_plan(flow_due_index_key(record), flow_due_index_key(next), record, next, {
      moves,
      deletes,
      puts
    })
  end

  defp flow_due_any_index_plan(record, next, {moves, deletes, puts}) do
    flow_due_index_plan(
      flow_due_any_index_key(record),
      flow_due_any_index_key(next),
      record,
      next,
      {
        moves,
        deletes,
        puts
      }
    )
  end

  defp flow_due_index_plan(from_key, to_key, record, next, {moves, deletes, puts}) do
    cond do
      is_binary(from_key) and is_binary(to_key) ->
        {[{from_key, to_key, next.id, Map.fetch!(next, :next_run_at_ms)} | moves], deletes, puts}

      is_binary(from_key) ->
        {moves, [{from_key, record.id} | deletes], puts}

      is_binary(to_key) ->
        puts = flow_claim_add_zset_entry(puts, to_key, next.id, Map.fetch!(next, :next_run_at_ms))
        {moves, deletes, puts}

      true ->
        {moves, deletes, puts}
    end
  end

  defp flow_claim_state_index_move(record, next) do
    from_key =
      FlowKeys.state_index_key(record.type, record.state, Map.get(record, :partition_key))

    to_key = FlowKeys.state_index_key(next.type, next.state, Map.get(next, :partition_key))

    {from_key, to_key, next.id, Map.get(next, :updated_at_ms, 0)}
  end

  defp flow_claim_metadata_index_moves(record, next) do
    score = Map.get(next, :updated_at_ms, 0)

    Enum.map(flow_metadata_index_entries(record), fn {key, id, _old_score} ->
      {key, key, id, score}
    end)
  end

  defp flow_claim_old_running_index_deletes(%{state: "running"} = record, deletes) do
    partition_key = Map.get(record, :partition_key)

    [
      {FlowKeys.inflight_index_key(record.type, partition_key), record.id},
      {FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key), record.id}
      | deletes
    ]
  end

  defp flow_claim_old_running_index_deletes(_record, deletes), do: deletes

  defp flow_claim_new_running_index_puts(%{state: "running"} = next, puts) do
    partition_key = Map.get(next, :partition_key)
    lease_score = Map.get(next, :lease_deadline_ms, 0)

    puts
    |> flow_claim_add_zset_entry(
      FlowKeys.inflight_index_key(next.type, partition_key),
      next.id,
      lease_score
    )
    |> flow_claim_add_zset_entry(
      FlowKeys.worker_index_key(Map.get(next, :lease_owner, ""), partition_key),
      next.id,
      lease_score
    )
  end

  defp flow_claim_new_running_index_puts(_next, puts), do: puts

  defp flow_claim_put_grouped_zset_entries(_state, puts) when map_size(puts) == 0, do: :ok

  defp flow_claim_put_grouped_zset_entries(state, puts) do
    Enum.each(puts, fn {key, member_score_pairs} ->
      flow_zset_put_many_new(state, key, Enum.reverse(member_score_pairs))
    end)

    :ok
  end

  defp flow_transition_move_indexes(_state, []), do: :ok

  defp flow_transition_move_indexes(state, plans) do
    with :ok <- flow_transition_move_due_indexes(state, plans),
         :ok <- flow_transition_move_state_indexes(state, plans),
         :ok <- flow_transition_move_metadata_indexes(state, plans),
         :ok <- flow_transition_delete_old_secondary_indexes(state, plans) do
      flow_transition_put_new_running_indexes(state, plans)
    end
  end

  defp flow_terminal_transition_move_indexes(state, plans) do
    with :ok <- flow_transition_move_due_indexes(state, plans),
         :ok <- flow_transition_move_state_indexes(state, plans),
         :ok <- flow_transition_move_metadata_indexes(state, plans) do
      flow_transition_delete_old_secondary_indexes(state, plans)
    end
  end

  defp flow_transition_move_due_indexes(state, plans) do
    {moves, deletes, puts} =
      Enum.reduce(plans, {[], [], %{}}, fn plan, {moves, deletes, puts} ->
        {record, next} = flow_claim_plan_pair(plan)
        {moves, deletes, puts} = flow_due_state_index_plan(record, next, {moves, deletes, puts})

        if flow_due_any_index_enabled?() do
          flow_due_any_index_plan(record, next, {moves, deletes, puts})
        else
          {moves, deletes, puts}
        end
      end)

    with :ok <- flow_index_move_lifecycle_entries(state, Enum.reverse(moves)),
         :ok <- flow_zset_lifecycle_index_delete_grouped(state, deletes) do
      puts
      |> Enum.each(fn {key, member_score_pairs} ->
        flow_index_put_new_lifecycle_members(state, key, Enum.reverse(member_score_pairs))
      end)

      :ok
    end
  end

  defp flow_due_index_key(%{next_run_at_ms: nil}), do: nil

  defp flow_due_index_key(%{type: type, state: flow_state, priority: priority} = record) do
    FlowKeys.due_key(type, flow_state, priority, Map.get(record, :partition_key))
  end

  defp flow_due_any_index_key(%{next_run_at_ms: nil}), do: nil

  defp flow_due_any_index_key(%{type: type, priority: priority} = record) do
    FlowKeys.due_any_key(type, priority, Map.get(record, :partition_key))
  end

  defp flow_transition_move_state_indexes(state, [plan]) do
    {record, next} = flow_claim_plan_pair(plan)

    from_key =
      FlowKeys.state_index_key(record.type, record.state, Map.get(record, :partition_key))

    to_key = FlowKeys.state_index_key(next.type, next.state, Map.get(next, :partition_key))

    flow_index_move_lifecycle_entries(
      state,
      [{from_key, to_key, next.id, Map.get(next, :updated_at_ms, 0)}]
    )
  end

  defp flow_transition_move_state_indexes(state, plans) do
    moves =
      Enum.map(plans, fn plan ->
        {record, next} = flow_claim_plan_pair(plan)

        from_key =
          FlowKeys.state_index_key(record.type, record.state, Map.get(record, :partition_key))

        to_key = FlowKeys.state_index_key(next.type, next.state, Map.get(next, :partition_key))
        {from_key, to_key, next.id, Map.get(next, :updated_at_ms, 0)}
      end)

    flow_index_move_lifecycle_entries(state, moves)
  end

  defp flow_transition_move_metadata_indexes(state, [plan]) do
    {record, next} = flow_claim_plan_pair(plan)

    case flow_transition_metadata_index_plan(record, next, [], [], []) do
      {[], [], []} ->
        :ok

      {moves, deletes, puts} ->
        with :ok <- flow_index_move_entries(state, Enum.reverse(moves)),
             :ok <- flow_zset_index_delete_grouped(state, deletes) do
          flow_index_put_new_entries(state, Enum.reverse(puts))
        end
    end
  end

  defp flow_transition_move_metadata_indexes(state, plans) do
    {moves, deletes, puts} =
      Enum.reduce(plans, {[], [], []}, fn plan, {moves, deletes, puts} ->
        {record, next} = flow_claim_plan_pair(plan)
        flow_transition_metadata_index_plan(record, next, moves, deletes, puts)
      end)

    case {moves, deletes, puts} do
      {[], [], []} ->
        :ok

      _ ->
        with :ok <- flow_index_move_entries(state, Enum.reverse(moves)),
             :ok <- flow_zset_index_delete_grouped(state, deletes) do
          flow_index_put_new_entries(state, Enum.reverse(puts))
        end
    end
  end

  defp flow_transition_metadata_index_plan(record, next, moves, deletes, puts) do
    if flow_metadata_index_record_empty?(record) and flow_metadata_index_record_empty?(next) do
      {moves, deletes, puts}
    else
      old_entries_list = flow_metadata_index_entries(record)
      new_entries_list = flow_metadata_index_entries(next)

      case {old_entries_list, new_entries_list} do
        {[], []} ->
          {moves, deletes, puts}

        _ ->
          old_entries =
            Map.new(old_entries_list, fn {key, id, score} ->
              {key, {id, score}}
            end)

          new_entries =
            Map.new(new_entries_list, fn {key, id, score} -> {key, {id, score}} end)

          moves =
            Enum.reduce(new_entries, moves, fn {key, {id, score}}, acc ->
              if Map.has_key?(old_entries, key) do
                [{key, key, id, score} | acc]
              else
                acc
              end
            end)

          deletes =
            Enum.reduce(old_entries, deletes, fn {key, {id, _score}}, acc ->
              if Map.has_key?(new_entries, key), do: acc, else: [{key, id} | acc]
            end)

          puts =
            Enum.reduce(new_entries, puts, fn {key, {id, score}}, acc ->
              if Map.has_key?(old_entries, key), do: acc, else: [{key, id, score} | acc]
            end)

          {moves, deletes, puts}
      end
    end
  end

  defp flow_metadata_index_record_empty?(%{
         id: id,
         parent_flow_id: parent_flow_id,
         root_flow_id: root_flow_id,
         correlation_id: correlation_id
       }) do
    flow_metadata_index_empty?(parent_flow_id, root_flow_id, correlation_id, id)
  end

  defp flow_metadata_index_record_empty?(record) do
    id = Map.get(record, :id)

    flow_metadata_index_empty?(
      Map.get(record, :parent_flow_id),
      Map.get(record, :root_flow_id),
      Map.get(record, :correlation_id),
      id
    )
  end

  defp flow_transition_delete_old_secondary_indexes(state, [plan]) do
    {record, _next} = flow_claim_plan_pair(plan)

    cond do
      Map.get(record, :state) == "running" ->
        partition_key = Map.get(record, :partition_key)

        flow_zset_lifecycle_index_delete_grouped(state, [
          {FlowKeys.inflight_index_key(record.type, partition_key), record.id},
          {FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key), record.id}
        ])

      Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) ->
        maybe_queue_terminal_lmdb_index_delete(state, record)
        queue_lmdb_metadata_index_deletes(state, record)

      true ->
        :ok
    end

    :ok
  end

  defp flow_transition_delete_old_secondary_indexes(state, plans) do
    {terminal_records, running_deletes} =
      Enum.reduce(plans, {[], []}, fn plan, {terminal_records, running_deletes} ->
        {record, _next} = flow_claim_plan_pair(plan)

        if Map.get(record, :state) == "running" do
          partition_key = Map.get(record, :partition_key)

          running_deletes = [
            {FlowKeys.inflight_index_key(record.type, partition_key), record.id},
            {FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key),
             record.id}
            | running_deletes
          ]

          {terminal_records, running_deletes}
        else
          if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
            {[record | terminal_records], running_deletes}
          else
            {terminal_records, running_deletes}
          end
        end
      end)

    Enum.each(terminal_records, fn record ->
      maybe_queue_terminal_lmdb_index_delete(state, record)
      queue_lmdb_metadata_index_deletes(state, record)
    end)

    flow_zset_lifecycle_index_delete_grouped(state, running_deletes)
  end

  defp flow_transition_put_new_running_indexes(state, [plan]) do
    {_record, next} = flow_claim_plan_pair(plan)

    if Map.get(next, :state) == "running" do
      flow_claim_put_running_indexes(state, [plan])
    else
      :ok
    end
  end

  defp flow_transition_put_new_running_indexes(state, plans) do
    plans
    |> Enum.filter(fn plan ->
      {_record, next} = flow_claim_plan_pair(plan)
      Map.get(next, :state) == "running"
    end)
    |> then(&flow_claim_put_running_indexes(state, &1))
  end

  defp flow_claim_put_state_records(state, plans) do
    case flow_claim_put_native_state_records_batch(state, plans) do
      :ok ->
        :ok

      :fallback ->
        case flow_claim_put_state_records_batch(state, plans) do
          :ok ->
            :ok

          :fallback ->
            flow_claim_put_state_records_loop(state, plans, nil)
            :ok
        end
    end
  end

  defp flow_claim_put_native_state_records_batch(
         state,
         [{:native_claim, _next, _entry, _state_key, _next_value, _previous_history_ms} | _] =
           plans
       ) do
    cond do
      cross_shard_pending_active?() ->
        :fallback

      standalone_staged_apply?() ->
        :fallback

      true ->
        lmdb_mirror? = flow_lmdb_mirror?(state)

        with {:ok, staged_entries} <-
               flow_claim_stage_native_state_record_entries(
                 plans,
                 lmdb_mirror?,
                 []
               ) do
          entries =
            Enum.map(staged_entries, fn {state_key, next, next_value, disk_val, entry} ->
              previous = :ets.lookup(state.ets, state_key)
              originals = Process.get(:sm_pending_originals, %{})
              updated = record_pending_original_from_previous(state_key, previous, originals)

              if updated != originals do
                Process.put(:sm_pending_originals, updated)
              end

              track_keydir_binary_delta_from_previous(state, state_key, previous, next_value, 0)
              maybe_queue_lmdb_policy_put(state_key, disk_val, 0)

              if lmdb_mirror? do
                maybe_queue_lmdb_indexes_for_state_record(state, state_key, next_value, 0, next)
              end

              queue_pending_put(state_key, disk_val, 0)

              entry
            end)

          :ets.insert(state.ets, entries)
          :ok
        end
    end
  end

  defp flow_claim_put_native_state_records_batch(_state, _plans), do: :fallback

  defp flow_claim_stage_native_state_record_entries([], _lmdb_mirror?, acc),
    do: {:ok, Enum.reverse(acc)}

  defp flow_claim_stage_native_state_record_entries(
         [
           {:native_claim, next, _entry, state_key, next_value, _previous_history_ms}
           | rest
         ],
         lmdb_mirror?,
         acc
       ) do
    if lmdb_mirror? and Ferricstore.Flow.LMDB.terminal_state?(Map.get(next, :state)) do
      :fallback
    else
      disk_val = to_disk_binary(next_value)
      entry = {state_key, next_value, 0, LFU.initial(), :pending, 0, byte_size(disk_val)}

      flow_claim_stage_native_state_record_entries(rest, lmdb_mirror?, [
        {state_key, next, next_value, disk_val, entry} | acc
      ])
    end
  end

  defp flow_claim_stage_native_state_record_entries(_plans, _lmdb_mirror?, _acc), do: :fallback

  defp flow_claim_put_state_records_batch(state, plans) do
    case flow_claim_state_record_key_records(plans) do
      {:ok, key_records} -> flow_put_state_records_batch(state, key_records)
      :fallback -> :fallback
    end
  end

  defp flow_claim_state_record_key_records(plans) do
    plans
    |> Enum.reduce_while({:ok, [], nil}, fn
      {:native_claim, _next, _entry, _state_key, _next_value, _previous_history_ms}, _acc ->
        {:halt, :fallback}

      {_record, next, _from_due_score}, {:ok, acc, cache} ->
        {key, cache} = flow_state_record_key(cache, next)
        {:cont, {:ok, [{key, next} | acc], cache}}

      {_record, next, _history_meta, _attrs}, {:ok, acc, cache} ->
        {key, cache} = flow_state_record_key(cache, next)
        {:cont, {:ok, [{key, next} | acc], cache}}

      {_record, next}, {:ok, acc, cache} ->
        {key, cache} = flow_state_record_key(cache, next)
        {:cont, {:ok, [{key, next} | acc], cache}}

      _other, _acc ->
        {:halt, :fallback}
    end)
    |> case do
      {:ok, key_records, _cache} -> {:ok, Enum.reverse(key_records)}
      :fallback -> :fallback
    end
  end

  defp flow_claim_put_state_records_loop(_state, [], _cache), do: :ok

  defp flow_claim_put_state_records_loop(
         state,
         [{:native_claim, next, _entry, state_key, next_value, _previous_history_ms} | rest],
         cache
       ) do
    flow_put_state_record_encoded(state, state_key, next_value, 0, next)
    flow_claim_put_state_records_loop(state, rest, cache)
  end

  defp flow_claim_put_state_records_loop(state, [{_record, next, _from_due_score} | rest], cache) do
    {key, cache} = flow_state_record_key(cache, next)
    flow_put_state_record(state, key, next)
    flow_claim_put_state_records_loop(state, rest, cache)
  end

  defp flow_claim_put_state_records_loop(
         state,
         [{_record, next, _history_meta, _attrs} | rest],
         cache
       ) do
    {key, cache} = flow_state_record_key(cache, next)
    flow_put_state_record(state, key, next)
    flow_claim_put_state_records_loop(state, rest, cache)
  end

  defp flow_claim_put_state_records_loop(state, [{_record, next} | rest], cache) do
    {key, cache} = flow_state_record_key(cache, next)
    flow_put_state_record(state, key, next)
    flow_claim_put_state_records_loop(state, rest, cache)
  end

  defp flow_state_record_key(cache, %{id: id} = record) do
    partition_key = Map.get(record, :partition_key)

    case cache do
      {^partition_key, prefix} when is_binary(prefix) ->
        {prefix <> id, cache}

      _ ->
        prefix = FlowKeys.state_key("", partition_key)
        {prefix <> id, {partition_key, prefix}}
    end
  end

  defp flow_state_key_with_tag(tag, id), do: "f:" <> tag <> ":s:" <> id
  defp flow_history_key_with_tag(tag, id), do: "f:" <> tag <> ":h:" <> id

  defp flow_due_key_with_tag(tag, type, flow_state, priority) do
    "f:" <> tag <> ":d:" <> type <> ":" <> flow_state <> ":p" <> Integer.to_string(priority)
  end

  defp flow_due_any_key_with_tag(tag, type, priority) do
    "f:" <> tag <> ":da:" <> type <> ":p" <> Integer.to_string(priority)
  end

  defp flow_state_index_key_with_tag(tag, type, flow_state) do
    "f:" <> tag <> ":i:s:" <> type <> ":" <> flow_state
  end

  defp flow_inflight_index_key_with_tag(tag, type), do: "f:" <> tag <> ":i:r:" <> type
  defp flow_worker_index_key_with_tag(tag, worker), do: "f:" <> tag <> ":i:w:" <> worker
  defp flow_parent_index_key_with_tag(tag, parent_id), do: "f:" <> tag <> ":i:p:" <> parent_id
  defp flow_root_index_key_with_tag(tag, root_id), do: "f:" <> tag <> ":i:o:" <> root_id

  defp flow_correlation_index_key_with_tag(tag, correlation_id),
    do: "f:" <> tag <> ":i:c:" <> correlation_id

  defp flow_create_put_state_records(state, records) do
    {key_records, _cache} =
      Enum.map_reduce(records, nil, fn record, cache ->
        {key, cache} = flow_state_record_key(cache, record)
        {{key, record}, cache}
      end)

    case flow_put_state_records_batch(state, key_records) do
      :ok ->
        :ok

      :fallback ->
        Enum.each(key_records, fn {key, record} ->
          flow_put_new_state_record(state, key, record)
        end)

        :ok
    end
  end

  defp flow_create_put_fast_state_records(state, plans) do
    key_records = Enum.map(plans, fn %{state_key: key, record: record} -> {key, record} end)

    case flow_put_state_records_batch(state, key_records) do
      :ok ->
        :ok

      :fallback ->
        Enum.each(key_records, fn {key, record} ->
          flow_put_new_state_record(state, key, record)
        end)

        :ok
    end
  end

  defp flow_put_state_records_batch(_state, []), do: :ok

  defp flow_put_state_records_batch(state, key_records) do
    cond do
      cross_shard_pending_active?() ->
        :fallback

      standalone_staged_apply?() ->
        :fallback

      true ->
        mirror? = flow_lmdb_mirror?(state)
        originals = Process.get(:sm_pending_originals, %{})
        pending_values = Process.get(:sm_pending_values, %{})

        case Enum.reduce_while(
               key_records,
               {:ok, [], [], originals, pending_values},
               fn {key, record}, {:ok, entries, writes, originals, pending_values} ->
                 value = flow_encode(record)
                 expire_at_ms = flow_record_expire_at(record)
                 previous = :ets.lookup(state.ets, key)
                 originals = record_pending_original_from_previous(key, previous, originals)

                 case maybe_externalize_apply_value(state, value) do
                   {:ok, :value, stored_value} ->
                     flow_stage_state_record_batch_entry(
                       state,
                       mirror?,
                       key,
                       record,
                       value,
                       stored_value,
                       stored_value,
                       expire_at_ms,
                       previous,
                       entries,
                       writes,
                       originals,
                       pending_values
                     )

                   {:ok, :blob_ref, stored_value, pending_value} ->
                     flow_stage_state_record_batch_entry(
                       state,
                       mirror?,
                       key,
                       record,
                       value,
                       stored_value,
                       pending_value,
                       expire_at_ms,
                       previous,
                       entries,
                       writes,
                       originals,
                       pending_values
                     )

                   {:error, _reason} = error ->
                     {:halt, error}
                 end
               end
             ) do
          {:ok, entries, writes, originals, pending_values} ->
            Process.put(:sm_pending_originals, originals)
            Process.put(:sm_pending_values, pending_values)
            Process.put(:sm_pending_writes, writes ++ Process.get(:sm_pending_writes, []))

            :ets.insert(state.ets, entries)
            :ok

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp flow_stage_state_record_batch_entry(
         state,
         mirror?,
         key,
         record,
         encoded_record,
         stored_value,
         pending_value,
         expire_at_ms,
         previous,
         entries,
         writes,
         originals,
         pending_values
       ) do
    terminal? = Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state))
    disk_val = to_disk_binary(stored_value)
    blob_ref? = stored_value != encoded_record

    {ets_value, lfu, write} =
      cond do
        mirror? and terminal? ->
          lfu = flow_record_lfu(record, encoded_record)

          maybe_queue_lmdb_indexes_for_state_record(
            state,
            key,
            encoded_record,
            expire_at_ms,
            record
          )

          {nil, lfu, {:put_cold, key, disk_val, expire_at_ms, lfu}}

        blob_ref? ->
          {nil, LFU.initial(), {:put_cold, key, disk_val, expire_at_ms, LFU.initial()}}

        true ->
          maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
          {encoded_record, LFU.initial(), {:put, key, disk_val, expire_at_ms}}
      end

    track_keydir_binary_delta_from_previous(
      state,
      key,
      previous,
      ets_value,
      expire_at_ms
    )

    entry = {key, ets_value, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
    pending_values = Map.put(pending_values, key, {pending_value, expire_at_ms})

    {:cont, {:ok, [entry | entries], [write | writes], originals, pending_values}}
  end

  defp flow_claim_put_running_indexes(state, plans) do
    plans
    |> Enum.reduce(%{}, fn plan, acc ->
      {_record, next} = flow_claim_plan_pair(plan)
      partition_key = Map.get(next, :partition_key)
      lease_score = Map.get(next, :lease_deadline_ms, 0)

      acc
      |> flow_claim_add_zset_entry(
        FlowKeys.inflight_index_key(next.type, partition_key),
        next.id,
        lease_score
      )
      |> flow_claim_add_zset_entry(
        FlowKeys.worker_index_key(Map.get(next, :lease_owner, ""), partition_key),
        next.id,
        lease_score
      )
    end)
    |> Enum.each(fn {key, member_score_pairs} ->
      flow_zset_put_many_new(state, key, Enum.reverse(member_score_pairs))
    end)

    :ok
  end

  defp flow_index_put_many_new(state, records) do
    records
    |> flow_index_grouped_entries()
    |> Enum.each(fn {key, member_score_pairs} ->
      flow_zset_put_many_new(state, key, Enum.reverse(member_score_pairs))
    end)

    flow_index_put_new_entries(state, Enum.flat_map(records, &flow_metadata_index_entries/1))

    :ok
  end

  defp flow_create_put_fast_due(state, plans) do
    plans
    |> Enum.reduce(%{}, fn %{record: record} = plan, acc ->
      acc
      |> flow_create_fast_due_entry(Map.get(plan, :due_key), record)
      |> flow_create_fast_due_entry(Map.get(plan, :due_any_key), record)
    end)
    |> flow_put_lifecycle_index_groups(state)
  end

  defp flow_create_put_fast_indexes(state, plans) do
    lifecycle_groups =
      Enum.reduce(plans, %{}, fn %{record: record} = plan, acc ->
        acc =
          flow_claim_add_zset_entry(
            acc,
            plan.state_index_key,
            record.id,
            plan.state_index_score
          )

        acc =
          acc
          |> flow_create_fast_due_entry(plan.due_key, record)
          |> flow_create_fast_due_entry(plan.due_any_key, record)

        Enum.reduce(plan.running_index_entries, acc, fn {key, score}, inner_acc ->
          flow_claim_add_zset_entry(inner_acc, key, record.id, score)
        end)
      end)

    with :ok <- flow_put_lifecycle_index_groups(lifecycle_groups, state) do
      plans
      |> Enum.flat_map(fn %{metadata_index_entries: entries} -> entries end)
      |> then(&flow_index_put_new_entries(state, &1))
    end
  end

  defp flow_create_fast_due_entry(acc, nil, _record), do: acc

  defp flow_create_fast_due_entry(acc, key, %{id: id, next_run_at_ms: score}) do
    flow_claim_add_zset_entry(acc, key, id, score)
  end

  defp flow_put_lifecycle_index_groups(groups, state) do
    Enum.each(groups, fn {key, member_score_pairs} ->
      flow_index_put_new_lifecycle_members(state, key, Enum.reverse(member_score_pairs))
    end)

    :ok
  end

  defp flow_index_grouped_entries(records) do
    records
    |> Enum.reduce(%{}, fn record, acc ->
      partition_key = Map.get(record, :partition_key)
      updated_score = Map.get(record, :updated_at_ms, 0)

      acc =
        flow_claim_add_zset_entry(
          acc,
          FlowKeys.state_index_key(record.type, record.state, partition_key),
          record.id,
          updated_score
        )

      acc =
        if Map.get(record, :state) == "running" do
          lease_score = Map.get(record, :lease_deadline_ms, 0)

          acc
          |> flow_claim_add_zset_entry(
            FlowKeys.inflight_index_key(record.type, partition_key),
            record.id,
            lease_score
          )
          |> flow_claim_add_zset_entry(
            FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key),
            record.id,
            lease_score
          )
        else
          acc
        end

      acc
    end)
  end

  defp flow_claim_add_zset_entry(acc, key, member, score) do
    Map.update(acc, key, [{member, score}], &[{member, score} | &1])
  end

  defp flow_claim_put_history(state, plans, now_ms) do
    flow_with_forced_async_history(fn ->
      flow_claim_put_history_batch(state, plans, now_ms)
    end)
  end

  defp flow_claim_put_history_batch(_state, [], _now_ms), do: :ok

  defp flow_claim_put_history_batch(state, plans, now_ms) do
    if flow_async_history_enabled?(state) do
      {projection_entries, after_history_records} =
        flow_claim_async_history_entries(state, plans, now_ms, [], [])

      with :ok <- queue_pending_flow_history_projections_batch(projection_entries) do
        flow_claim_after_history_put_records_batch(state, after_history_records)
      end
    else
      history_entries =
        Enum.map(plans, fn
          {:native_claim, next, _entry, _state_key, _value, previous_history_ms} ->
            flow_history_put_ready_entry(
              state,
              next,
              "claimed",
              now_ms,
              previous_history_ms
            )

          plan ->
            {record, next} = flow_claim_plan_pair(plan)

            flow_history_put_ready_entry(
              state,
              next,
              "claimed",
              now_ms,
              flow_previous_history_ms(record)
            )
        end)

      with :ok <- flow_history_index_put_entries(state, history_entries) do
        flow_claim_after_history_put_batch(state, plans)
      end
    end
  end

  defp flow_claim_async_history_entries(_state, [], _now_ms, entries, after_history_records) do
    {Enum.reverse(entries), Enum.reverse(after_history_records)}
  end

  defp flow_claim_async_history_entries(
         state,
         [{:native_claim, next, _entry, _state_key, _value, previous_history_ms} | rest],
         now_ms,
         entries,
         after_history_records
       ) do
    entry = flow_claim_async_history_entry(state, next, now_ms, previous_history_ms)
    after_history_records = flow_claim_after_history_record_acc(next, after_history_records)

    flow_claim_async_history_entries(
      state,
      rest,
      now_ms,
      [entry | entries],
      after_history_records
    )
  end

  defp flow_claim_async_history_entries(
         state,
         [plan | rest],
         now_ms,
         entries,
         after_history_records
       ) do
    {record, next} = flow_claim_plan_pair(plan)
    entry = flow_claim_async_history_entry(state, next, now_ms, flow_previous_history_ms(record))
    after_history_records = flow_claim_after_history_record_acc(next, after_history_records)

    flow_claim_async_history_entries(
      state,
      rest,
      now_ms,
      [entry | entries],
      after_history_records
    )
  end

  defp flow_claim_after_history_record_acc(record, acc) do
    if flow_claim_after_history_fast_record?(record), do: acc, else: [record | acc]
  end

  defp flow_claim_async_history_entry(
         state,
         %{id: id, version: version} = record,
         now_ms,
         previous_history_ms
       ) do
    partition_key = Map.get(record, :partition_key)
    history_key = FlowKeys.history_key(id, partition_key)

    {event_id, event_ms} =
      flow_history_next_event(state, history_key, now_ms, version, previous_history_ms)

    compound_key = FlowKeys.stream_entry_key_from_history_key(history_key, event_id)

    %{
      key: compound_key,
      expire_at_ms: 0,
      history_key: history_key,
      event_id: event_id,
      event_ms: event_ms,
      version: version,
      history_max_events: Map.get(record, :history_max_events),
      record: record,
      event: "claimed",
      now_ms: now_ms,
      meta: %{}
    }
  end

  defp flow_claim_after_history_put_batch(state, plans) do
    records =
      Enum.map(plans, fn plan ->
        {_record, next} = flow_claim_plan_pair(plan)
        next
      end)

    flow_claim_after_history_put_records_batch(state, records)
  end

  defp flow_claim_after_history_put_records_batch(state, records) do
    if Enum.all?(records, &flow_claim_after_history_fast_record?/1) do
      :ok
    else
      flow_after_history_put_many(records, state)
    end
  end

  defp flow_claim_after_history_fast_record?(%{state: "running"} = record) do
    flow_history_trim_skippable?(record)
  end

  defp flow_claim_after_history_fast_record?(_record), do: false

  defp flow_history_trim_skippable?(%{history_max_events: nil}), do: true

  defp flow_history_trim_skippable?(%{history_max_events: max}) when not is_integer(max),
    do: true

  defp flow_history_trim_skippable?(%{history_max_events: max, version: version})
       when is_integer(version) and version <= max,
       do: true

  defp flow_history_trim_skippable?(_record), do: false

  defp flow_transition_put_history(state, plans) do
    flow_many_put_history(state, plans, "transitioned")
  end

  defp flow_many_put_history(state, plans, event) do
    flow_with_forced_async_history(fn ->
      history_entries =
        Enum.map(plans, fn plan ->
          {record, next} = flow_claim_plan_pair(plan)

          flow_history_put_ready_entry(
            state,
            next,
            event,
            Map.get(next, :updated_at_ms),
            flow_previous_history_ms(record)
          )
        end)

      with :ok <- flow_history_index_put_entries(state, history_entries) do
        plans
        |> Enum.map(fn plan ->
          {_record, next} = flow_claim_plan_pair(plan)
          next
        end)
        |> flow_after_history_put_many(state)
      end
    end)
  end

  defp flow_retry_many_put_history(state, plans) do
    flow_with_forced_async_history(fn ->
      history_entries =
        Enum.map(plans, fn plan ->
          {record, next, history_meta} = flow_retry_history_plan(plan)

          flow_history_put_ready_entry(
            state,
            next,
            "retry",
            Map.get(next, :updated_at_ms),
            flow_previous_history_ms(record),
            history_meta
          )
        end)

      with :ok <- flow_history_index_put_entries(state, history_entries) do
        plans
        |> Enum.map(fn plan ->
          {_record, next} = flow_claim_plan_pair(plan)
          next
        end)
        |> flow_after_history_put_many(state)
      end
    end)
  end

  defp flow_retry_history_plan({record, next, history_meta, _attrs}),
    do: {record, next, history_meta}

  defp flow_retry_history_plan({record, next, history_meta}), do: {record, next, history_meta}

  defp flow_create_put_history(state, records) do
    history_entries =
      Enum.map(records, fn record ->
        flow_history_put_ready_entry(
          state,
          record,
          "created",
          Map.get(record, :created_at_ms),
          nil
        )
      end)

    with :ok <- flow_history_index_put_entries(state, history_entries) do
      flow_after_history_put_many(records, state)
    end
  end

  defp flow_create_put_fast_history(state, plans) do
    history_entries =
      Enum.map(plans, fn %{record: record, history_key: history_key} ->
        flow_history_put_ready_entry_with_key(
          state,
          record,
          history_key,
          "created",
          Map.get(record, :created_at_ms),
          nil,
          %{}
        )
      end)

    with :ok <- flow_history_index_put_entries(state, history_entries) do
      plans
      |> Enum.map(fn %{record: record} -> record end)
      |> flow_after_history_put_many(state)
    end
  end

  defp flow_require_record(state, id, partition_key) do
    case flow_read_record(state, id, partition_key) do
      nil -> {:error, "ERR flow not found"}
      record -> {:ok, record}
    end
  end

  defp flow_history_put_ready_entry(
         state,
         record,
         event,
         now_ms,
         previous_history_ms
       ) do
    flow_history_put_ready_entry(state, record, event, now_ms, previous_history_ms, %{})
  end

  defp flow_history_put_ready_entry(
         state,
         %{id: id, version: _version} = record,
         event,
         now_ms,
         previous_history_ms,
         meta
       ) do
    partition_key = Map.get(record, :partition_key)
    history_key = FlowKeys.history_key(id, partition_key)

    flow_history_put_ready_entry_with_key(
      state,
      record,
      history_key,
      event,
      now_ms,
      previous_history_ms,
      meta
    )
  end

  defp flow_history_put_ready_entry_with_key(
         state,
         %{version: version} = record,
         history_key,
         event,
         now_ms,
         previous_history_ms,
         meta
       ) do
    {event_id, event_ms} =
      flow_history_next_event(state, history_key, now_ms, version, previous_history_ms)

    compound_key = FlowKeys.stream_entry_key_from_history_key(history_key, event_id)

    entry = %{
      key: compound_key,
      expire_at_ms: 0,
      history_key: history_key,
      event_id: event_id,
      event_ms: event_ms,
      version: version,
      history_max_events: Map.get(record, :history_max_events),
      record: record,
      event: event,
      now_ms: now_ms,
      meta: meta
    }

    :ok = flow_history_put_or_queue_entry(state, entry)

    {history_key, event_id, event_ms}
  end

  defp flow_require_expected_state(_record, nil), do: :ok
  defp flow_require_expected_state(%{state: expected_state}, expected_state), do: :ok
  defp flow_require_expected_state(_record, _expected_state), do: {:error, "ERR flow wrong state"}

  defp flow_require_running_lease(%{state: "running", lease_token: token}, token), do: :ok
  defp flow_require_running_lease(_record, _token), do: {:error, "ERR stale flow lease"}

  defp flow_require_fencing_token(record, fencing_token) do
    if Map.get(record, :fencing_token, 0) == fencing_token do
      :ok
    else
      {:error, "ERR stale flow lease"}
    end
  end

  defp flow_require_transition_lease(%{lease_token: nil}, nil), do: :ok
  defp flow_require_transition_lease(%{lease_token: token}, token), do: :ok
  defp flow_require_transition_lease(_record, _token), do: {:error, "ERR stale flow lease"}

  defp flow_require_rewindable(%{lease_token: token}) when is_binary(token),
    do: {:error, "ERR flow cannot rewind leased flow"}

  defp flow_require_rewindable(%{parent_flow_id: parent_id})
       when is_binary(parent_id) and parent_id != "",
       do: {:error, "ERR flow cannot rewind parent or child flow"}

  defp flow_require_rewindable(%{child_groups: groups})
       when is_map(groups) and map_size(groups) > 0,
       do: {:error, "ERR flow cannot rewind parent or child flow"}

  defp flow_require_rewindable(_record), do: :ok

  defp flow_validate_record_keys(
         %{id: id, type: type, state: flow_state, priority: priority} = record
       ) do
    partition_key = Map.get(record, :partition_key)
    state_key = FlowKeys.state_key(id, partition_key)
    history_key = FlowKeys.history_key(id, partition_key)

    with :ok <- flow_validate_key_size(state_key),
         :ok <- flow_validate_key_size(history_key),
         :ok <- flow_validate_key_size(FlowKeys.state_index_key(type, flow_state, partition_key)),
         :ok <-
           flow_validate_key_size(
             FlowKeys.stream_entry_key_from_history_key(
               history_key,
               "18446744073709551615-18446744073709551615"
             )
           ) do
      with :ok <- flow_validate_due_key(record, type, flow_state, priority, partition_key),
           :ok <- flow_validate_running_index_keys(record, type, partition_key) do
        flow_validate_metadata_index_keys(record, partition_key)
      end
    end
  end

  defp flow_validate_terminal_state_index_key(%{type: type, state: flow_state} = record) do
    type
    |> FlowKeys.state_index_key(flow_state, Map.get(record, :partition_key))
    |> flow_validate_key_size()
  end

  defp flow_validate_claim_next_record_keys(
         %{type: type, state: flow_state, priority: priority} = record
       ) do
    partition_key = Map.get(record, :partition_key)

    with :ok <- flow_validate_key_size(FlowKeys.state_index_key(type, flow_state, partition_key)),
         :ok <- flow_validate_due_key(record, type, flow_state, priority, partition_key) do
      flow_validate_running_index_keys(record, type, partition_key)
    end
  end

  defp flow_validate_due_key(record, type, flow_state, priority, partition_key) do
    case Map.get(record, :next_run_at_ms) do
      nil ->
        :ok

      _ ->
        with :ok <-
               flow_validate_key_size(FlowKeys.due_key(type, flow_state, priority, partition_key)) do
          if flow_due_any_index_enabled?() do
            flow_validate_key_size(FlowKeys.due_any_key(type, priority, partition_key))
          else
            :ok
          end
        end
    end
  end

  defp flow_validate_running_index_keys(%{state: "running"} = record, type, partition_key) do
    with :ok <- flow_validate_key_size(FlowKeys.inflight_index_key(type, partition_key)) do
      flow_validate_key_size(
        FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key)
      )
    end
  end

  defp flow_validate_running_index_keys(_record, _type, _partition_key), do: :ok

  defp flow_validate_metadata_index_keys(record, partition_key) do
    id = Map.get(record, :id)

    [
      {Map.get(record, :parent_flow_id), &FlowKeys.parent_index_key(&1, partition_key)},
      {flow_non_default_root_flow_id(record, id), &FlowKeys.root_index_key(&1, partition_key)},
      {Map.get(record, :correlation_id), &FlowKeys.correlation_index_key(&1, partition_key)}
    ]
    |> Enum.reduce_while(:ok, fn
      {value, key_fun}, :ok when is_binary(value) and value != "" ->
        case flow_validate_key_size(key_fun.(value)) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end

      _entry, :ok ->
        {:cont, :ok}
    end)
  end

  defp flow_non_default_root_flow_id(record, id) do
    case Map.get(record, :root_flow_id) do
      ^id -> nil
      root_flow_id -> root_flow_id
    end
  end

  defp flow_validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  defp flow_read_record(state, id, partition_key) do
    key = FlowKeys.state_key(id, partition_key)

    flow_read_record_by_key(state, key)
  end

  defp flow_read_record_by_key(state, key) do
    cond do
      flow_lmdb_mirror?(state) ->
        case flow_read_mirror_record(state, key) do
          {:ok, record} -> record
          :miss -> flow_read_ets_record(state, key)
        end

      true ->
        flow_read_ets_record(state, key)
    end
  end

  defp flow_read_policy(_state, type) when not is_binary(type), do: nil

  defp flow_read_policy(state, type) do
    case ets_lookup(state, FlowKeys.policy_key(type)) do
      {:hit, value, _expire_at_ms} when is_binary(value) ->
        case RetryPolicy.decode_flow_policy(value) do
          {:ok, policy} -> policy
          :error -> nil
        end

      _ ->
        nil
    end
  end

  defp flow_read_records(state, attrs_list) do
    flow_read_records_by_keys(state, flow_state_keys_for_attrs(attrs_list))
  end

  defp flow_state_keys_for_attrs(attrs_list) do
    Enum.map(attrs_list, fn attrs ->
      FlowKeys.state_key(Map.fetch!(attrs, :id), Map.get(attrs, :partition_key))
    end)
  end

  defp flow_state_key_present?(state, key) do
    case flow_state_key_present_hot?(state, key) do
      true -> true
      false -> flow_lmdb_mirror?(state) and flow_lmdb_record_present?(state, key)
    end
  end

  defp flow_state_keys_present(state, keys) do
    hot_results = Enum.map(keys, &flow_state_key_present_hot?(state, &1))

    if flow_lmdb_mirror?(state) and Enum.any?(hot_results, &(&1 == false)) do
      lmdb_reads =
        keys
        |> Enum.zip(hot_results)
        |> Enum.with_index()
        |> Enum.flat_map(fn
          {{key, false}, idx} -> [{idx, key}]
          {_present, _idx} -> []
        end)

      lmdb_results =
        flow_lmdb_records_present(
          state,
          Enum.map(lmdb_reads, fn {_idx, key} -> key end)
        )

      lmdb_by_idx =
        lmdb_reads
        |> Enum.zip(lmdb_results)
        |> Map.new(fn {{idx, _key}, present?} -> {idx, present?} end)

      hot_results
      |> Enum.with_index()
      |> Enum.map(fn
        {true, _idx} -> true
        {false, idx} -> Map.get(lmdb_by_idx, idx, false)
      end)
    else
      hot_results
    end
  end

  defp flow_state_key_present_hot?(state, key) do
    case ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} when is_binary(value) -> true
      _ -> false
    end
  end

  defp flow_read_records_by_keys(state, keys) do
    cond do
      flow_lmdb_mirror?(state) ->
        flow_read_mirror_records(state, keys)

      true ->
        Enum.map(keys, &flow_read_hot_state_record(state, &1))
    end
  end

  defp flow_read_hot_state_record(state, key) do
    case :ets.lookup(state.ets, key) do
      [{^key, value, 0, _lfu, _fid, _off, _vsize}] when is_binary(value) ->
        flow_decode_hot_state_value(value)

      _ ->
        flow_read_ets_record(state, key)
    end
  end

  defp flow_read_mirror_records(state, keys) do
    ets_results = Enum.map(keys, &flow_read_ets_record(state, &1))

    lmdb_reads =
      keys
      |> Enum.zip(ets_results)
      |> Enum.with_index()
      |> Enum.flat_map(fn
        {{key, nil}, idx} -> [{idx, key}]
        {_present, _idx} -> []
      end)

    lmdb_results = flow_read_lmdb_records(state, Enum.map(lmdb_reads, fn {_idx, key} -> key end))

    results =
      lmdb_reads
      |> Enum.zip(lmdb_results)
      |> Enum.reduce(%{}, fn
        {{idx, _key}, {:ok, record}}, acc -> Map.put(acc, idx, record)
        {{idx, _key}, _result}, acc -> Map.put(acc, idx, nil)
      end)

    results =
      ets_results
      |> Enum.with_index()
      |> Enum.reduce(results, fn
        {nil, _idx}, acc ->
          acc

        {record, idx}, acc ->
          Map.put(acc, idx, record)
      end)

    keys
    |> Enum.with_index()
    |> Enum.map(fn {_key, idx} -> Map.get(results, idx) end)
  end

  defp flow_read_mirror_record(state, key) do
    case flow_read_ets_record(state, key) do
      nil ->
        flow_read_lmdb_record(state, key)

      record ->
        {:ok, record}
    end
  end

  defp flow_read_lmdb_records(_state, []), do: []

  defp flow_read_lmdb_records(state, keys) do
    pending = Process.get(:sm_pending_lmdb_values, %{})

    {results, lmdb_keys} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {key, idx}, {result_acc, read_acc} ->
        if Map.has_key?(pending, key) do
          result = flow_decode_pending_lmdb_record(pending, key)
          {Map.put(result_acc, idx, result), read_acc}
        else
          {result_acc, [{idx, key} | read_acc]}
        end
      end)

    lmdb_values =
      flow_lmdb_get_many(
        state,
        lmdb_keys
        |> Enum.reverse()
        |> Enum.map(fn {_idx, key} -> key end)
      )

    results =
      lmdb_keys
      |> Enum.reverse()
      |> Enum.zip(lmdb_values)
      |> Enum.reduce(results, fn {{idx, _key}, result}, acc -> Map.put(acc, idx, result) end)

    for idx <- 0..(length(keys) - 1)//1, do: Map.get(results, idx, :miss)
  end

  defp flow_lmdb_get_many(_state, []), do: []

  defp flow_lmdb_get_many(state, keys) do
    case Ferricstore.Flow.LMDB.get_many(flow_lmdb_record_path(state), keys) do
      {:ok, results} ->
        Enum.map(results, fn
          {:ok, blob} -> flow_decode_lmdb_blob(blob)
          :not_found -> :miss
          {:error, _reason} -> :miss
        end)

      {:error, _reason} ->
        Enum.map(keys, fn key ->
          case Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), key) do
            {:ok, blob} -> flow_decode_lmdb_blob(blob)
            :not_found -> :miss
            {:error, _reason} -> :miss
          end
        end)
    end
  end

  defp flow_lmdb_records_present(_state, []), do: []

  defp flow_lmdb_records_present(state, keys) do
    pending = Process.get(:sm_pending_lmdb_values, %{})

    {results, lmdb_keys} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {key, idx}, {result_acc, read_acc} ->
        if Map.has_key?(pending, key) do
          result = flow_pending_lmdb_record_present?(pending, key)
          {Map.put(result_acc, idx, result), read_acc}
        else
          {result_acc, [{idx, key} | read_acc]}
        end
      end)

    lmdb_results =
      flow_lmdb_get_many_present(
        state,
        lmdb_keys
        |> Enum.reverse()
        |> Enum.map(fn {_idx, key} -> key end)
      )

    results =
      lmdb_keys
      |> Enum.reverse()
      |> Enum.zip(lmdb_results)
      |> Enum.reduce(results, fn {{idx, _key}, present?}, acc -> Map.put(acc, idx, present?) end)

    for idx <- 0..(length(keys) - 1)//1, do: Map.get(results, idx, false)
  end

  defp flow_lmdb_get_many_present(_state, []), do: []

  defp flow_lmdb_get_many_present(state, keys) do
    case Ferricstore.Flow.LMDB.get_many(flow_lmdb_record_path(state), keys) do
      {:ok, results} ->
        Enum.map(results, fn
          {:ok, blob} -> flow_lmdb_blob_present?(blob)
          :not_found -> false
          {:error, _reason} -> false
        end)

      {:error, _reason} ->
        Enum.map(keys, fn key ->
          case Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), key) do
            {:ok, blob} -> flow_lmdb_blob_present?(blob)
            :not_found -> false
            {:error, _reason} -> false
          end
        end)
    end
  end

  defp flow_lmdb_record_present?(state, key) do
    case flow_lmdb_get_many_present(state, [key]) do
      [present?] -> present?
      _ -> false
    end
  end

  defp flow_pending_lmdb_record_present?(pending, key) do
    case Map.get(pending, key) do
      {:put, blob} -> flow_lmdb_blob_present?(blob)
      :delete -> false
      _ -> false
    end
  end

  defp flow_lmdb_blob_present?(blob) when is_binary(blob) do
    case Ferricstore.Flow.LMDB.decode_value(blob, apply_now_ms()) do
      {:ok, _value} -> true
      :expired -> false
      :error -> false
    end
  end

  defp flow_lmdb_blob_present?(_blob), do: false

  defp flow_read_ets_record(state, key) do
    case ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} when is_binary(value) ->
        flow_decode_hot_state_value(value)

      _ ->
        nil
    end
  end

  defp flow_decode_hot_state_value(value) when is_binary(value) do
    try do
      Flow.decode_record(value)
    rescue
      _ -> nil
    end
  end

  defp flow_read_lmdb_record(state, key) do
    cond do
      Map.has_key?(Process.get(:sm_pending_lmdb_values, %{}), key) ->
        flow_decode_pending_lmdb_record(Process.get(:sm_pending_lmdb_values, %{}), key)

      true ->
        case Ferricstore.Flow.LMDB.get(flow_lmdb_record_path(state), key) do
          {:ok, blob} -> flow_decode_lmdb_blob(blob)
          :not_found -> :miss
          {:error, _reason} -> :miss
        end
    end
  end

  defp flow_decode_pending_lmdb_record(pending, key) do
    case Map.get(pending, key) do
      {:put, blob} -> flow_decode_lmdb_blob(blob)
      :delete -> :miss
      _ -> :miss
    end
  end

  defp flow_decode_lmdb_blob(blob) do
    case Ferricstore.Flow.LMDB.decode_value(blob, apply_now_ms()) do
      {:ok, value} ->
        flow_decode_record_blob(value)

      :expired ->
        :miss

      :error ->
        :miss
    end
  end

  defp flow_decode_record_blob(value) when is_binary(value) do
    try do
      {:ok, Flow.decode_record(value)}
    rescue
      _ -> :miss
    end
  end

  defp flow_decode_record_blob(_value), do: :miss

  defp flow_history_event_fields(state, id, event_id, partition_key) do
    history_key = FlowKeys.history_key(id, partition_key)

    case flow_history_indexed_event_fields(state, id, partition_key, history_key, event_id) do
      {:ok, _fields} = ok -> ok
      :trimmed -> {:error, "ERR flow rewind target event not found"}
      :miss -> flow_history_scanned_event_fields(state, history_key, event_id)
    end
  end

  defp flow_history_indexed_event_fields(state, _id, _partition_key, history_key, event_id) do
    compound_key = FlowKeys.stream_entry_key_from_history_key(history_key, event_id)

    case flow_index_score_of(state, history_key, event_id) do
      {:ok, _score} ->
        case flow_history_lookup_value(state, compound_key) do
          {:hit, value, _expire_at_ms} ->
            {:ok, value |> flow_decode_history_fields() |> flow_history_fields_to_map()}

          _ ->
            :miss
        end

      :miss ->
        if flow_index_tables?(state), do: :trimmed, else: :miss

      _other ->
        :miss
    end
  end

  defp flow_history_scanned_event_fields(state, history_key, event_id) do
    prefix = "X:" <> history_key <> <<0>>
    target_key = prefix <> event_id

    case HistoryProjector.scan_event_value(state.shard_data_path, target_key) do
      {:ok, value} ->
        {:ok, value |> flow_decode_history_fields() |> flow_history_fields_to_map()}

      _ ->
        state
        |> shard_ets_state()
        |> Ferricstore.Store.Shard.ETS.prefix_scan_entries(prefix, state.shard_data_path)
        |> Enum.find(fn {entry_id, _value} -> prefix <> entry_id == target_key end)
        |> case do
          {_entry_id, value} ->
            {:ok, value |> flow_decode_history_fields() |> flow_history_fields_to_map()}

          nil ->
            {:error, "ERR flow rewind target event not found"}
        end
    end
  end

  defp flow_history_lookup_value(state, key) do
    case :ets.lookup(state.ets, key) do
      [{^key, nil, expire_at_ms, _lfu, {:flow_history, _file_id} = file_id, offset, _value_size}] ->
        case HistoryProjector.read_value(state.shard_data_path, file_id, offset) do
          {:ok, value} -> {:hit, value, expire_at_ms}
          _ -> :miss
        end

      _ ->
        ets_lookup(state, key)
    end
  rescue
    _ -> :miss
  end

  defp flow_decode_history_fields(value), do: Flow.decode_history_fields(value)

  defp flow_history_fields_to_map(fields) when is_list(fields) do
    fields
    |> Enum.chunk_every(2)
    |> Enum.reduce(%{}, fn
      [key, value], acc when is_binary(key) -> Map.put(acc, key, value)
      _pair, acc -> acc
    end)
  end

  defp flow_rewind_record(record, fields, attrs, now_ms) do
    with {:ok, target_state} <- flow_history_required_field(fields, "state"),
         {:ok, priority} <-
           flow_history_integer_field(fields, "priority", Map.get(record, :priority, 0)),
         {:ok, attempts} <-
           flow_history_integer_field(fields, "attempts", Map.get(record, :attempts, 0)),
         {:ok, history_run_at_ms} <- flow_history_optional_integer_field(fields, "next_run_at_ms"),
         {:ok, created_at_ms} <-
           flow_history_integer_field(
             fields,
             "created_at_ms",
             Map.get(record, :created_at_ms, now_ms)
           ) do
      next_run_at_ms =
        case Map.get(attrs, :run_at_ms) do
          value when is_integer(value) -> value
          _ -> history_run_at_ms
        end

      {:ok,
       record
       |> Map.merge(%{
         state: target_state,
         version: Map.fetch!(record, :version) + 1,
         attempts: attempts,
         fencing_token: Map.get(record, :fencing_token, 0) + 1,
         created_at_ms: created_at_ms,
         updated_at_ms: now_ms,
         next_run_at_ms: next_run_at_ms,
         priority: priority,
         payload_ref: flow_nilable_history_field(fields, "payload_ref"),
         parent_flow_id: flow_nilable_history_field(fields, "parent_flow_id"),
         root_flow_id: flow_nilable_history_field(fields, "root_flow_id") || Map.get(record, :id),
         correlation_id: flow_nilable_history_field(fields, "correlation_id"),
         result_ref: flow_nilable_history_field(fields, "result_ref"),
         error_ref:
           Map.get(attrs, :reason_ref) || flow_nilable_history_field(fields, "error_ref"),
         value_refs: flow_history_named_value_refs_field(fields),
         lease_owner: nil,
         lease_token: nil,
         lease_deadline_ms: 0
       })
       |> flow_stamp_terminal_retention(now_ms)}
    end
  end

  defp flow_history_required_field(fields, key) do
    case Map.get(fields, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow rewind target event cannot restore state"}
    end
  end

  defp flow_history_integer_field(fields, key, default) do
    case Map.get(fields, key) do
      nil -> {:ok, default}
      value -> flow_parse_history_integer(value)
    end
  end

  defp flow_history_optional_integer_field(fields, key) do
    case Map.get(fields, key) do
      nil -> {:ok, nil}
      "" -> {:ok, nil}
      value -> flow_parse_history_integer(value)
    end
  end

  defp flow_parse_history_integer(value) when is_integer(value), do: {:ok, value}

  defp flow_parse_history_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> {:ok, int}
      _ -> {:error, "ERR flow rewind target event cannot restore state"}
    end
  end

  defp flow_parse_history_integer(_value),
    do: {:error, "ERR flow rewind target event cannot restore state"}

  defp flow_history_named_value_refs_field(fields) do
    fields
    |> Map.get("value_refs")
    |> flow_normalize_value_refs()
  end

  defp flow_nilable_history_field(fields, key) do
    case Map.get(fields, key) do
      "" -> nil
      value -> value
    end
  end

  defp flow_due_put(_state, %{next_run_at_ms: nil}), do: :ok

  defp flow_due_put(state, %{type: type, state: flow_state, priority: priority, id: id} = record) do
    partition_key = Map.get(record, :partition_key)
    due_key = FlowKeys.due_key(type, flow_state, priority, partition_key)
    score = Map.fetch!(record, :next_run_at_ms)

    with :ok <- flow_index_put_new_lifecycle_members(state, due_key, [{id, score}]) do
      if flow_due_any_index_enabled?() do
        due_any_key = FlowKeys.due_any_key(type, priority, partition_key)
        flow_index_put_new_lifecycle_members(state, due_any_key, [{id, score}])
      else
        :ok
      end
    end
  end

  defp flow_zset_delete_members_from_key(_state, _due_key, []), do: :ok

  defp flow_zset_delete_members_from_key(state, due_key, ids) do
    flow_index_delete_lifecycle_members(state, due_key, Enum.uniq(ids))
  end

  defp flow_zset_index_delete_grouped(state, key_ids) do
    key_ids
    |> Enum.group_by(fn {key, _id} -> key end, fn {_key, id} -> id end)
    |> Enum.each(fn {key, ids} ->
      flow_zset_delete_many(state, key, Enum.uniq(ids))
    end)

    :ok
  end

  defp flow_zset_lifecycle_index_delete_grouped(_state, []), do: :ok

  defp flow_zset_lifecycle_index_delete_grouped(state, [{key, id}]) do
    flow_index_delete_lifecycle_members(state, key, [id])
  end

  defp flow_zset_lifecycle_index_delete_grouped(state, [{key, id1}, {key, id2}]) do
    ids = if id1 == id2, do: [id1], else: [id1, id2]
    flow_index_delete_lifecycle_members(state, key, ids)
  end

  defp flow_zset_lifecycle_index_delete_grouped(state, [{key1, id1}, {key2, id2}]) do
    flow_index_delete_lifecycle_members(state, key1, [id1])
    flow_index_delete_lifecycle_members(state, key2, [id2])
  end

  defp flow_zset_lifecycle_index_delete_grouped(state, key_ids) do
    key_ids
    |> Enum.group_by(fn {key, _id} -> key end, fn {_key, id} -> id end)
    |> Enum.each(fn {key, ids} ->
      flow_index_delete_lifecycle_members(state, key, Enum.uniq(ids))
    end)

    :ok
  end

  defp flow_index_put(state, %{id: id, type: type, state: flow_state} = record) do
    partition_key = Map.get(record, :partition_key)
    state_index_key = FlowKeys.state_index_key(type, flow_state, partition_key)
    updated_score = Map.get(record, :updated_at_ms, 0)

    with :ok <-
           flow_index_put_new_lifecycle_members(state, state_index_key, [{id, updated_score}]),
         :ok <- flow_metadata_index_put(state, record, updated_score) do
      flow_running_index_put(state, record)
    end
  end

  defp flow_metadata_index_put(state, record, _score) do
    flow_index_put_new_entries(state, flow_metadata_index_entries(record))
  end

  defp flow_metadata_index_entries(record) do
    id = Map.get(record, :id)
    parent_flow_id = Map.get(record, :parent_flow_id)
    root_flow_id = Map.get(record, :root_flow_id)
    correlation_id = Map.get(record, :correlation_id)

    if flow_metadata_index_empty?(parent_flow_id, root_flow_id, correlation_id, id) do
      []
    else
      partition_key = Map.get(record, :partition_key)
      score = Map.get(record, :updated_at_ms, 0)

      []
      |> flow_metadata_index_entry(:parent, parent_flow_id, partition_key, id, score)
      |> flow_metadata_index_entry(:root, root_flow_id, partition_key, id, score)
      |> flow_metadata_index_entry(:correlation, correlation_id, partition_key, id, score)
    end
  end

  defp flow_metadata_index_empty?(parent_flow_id, root_flow_id, correlation_id, id) do
    flow_blank_metadata?(parent_flow_id) and flow_blank_metadata?(correlation_id) and
      flow_blank_or_same_metadata?(root_flow_id, id)
  end

  defp flow_blank_metadata?(nil), do: true
  defp flow_blank_metadata?(""), do: true
  defp flow_blank_metadata?(_value), do: false

  defp flow_blank_or_same_metadata?(nil, _id), do: true
  defp flow_blank_or_same_metadata?("", _id), do: true
  defp flow_blank_or_same_metadata?(value, id), do: value == id

  defp flow_metadata_index_entry(entries, :root, nil, _partition_key, _id, _score), do: entries
  defp flow_metadata_index_entry(entries, :root, "", _partition_key, _id, _score), do: entries
  defp flow_metadata_index_entry(entries, :root, id, _partition_key, id, _score), do: entries

  defp flow_metadata_index_entry(entries, kind, value, partition_key, id, score)
       when is_binary(value) and value != "" do
    key =
      case kind do
        :parent -> FlowKeys.parent_index_key(value, partition_key)
        :root -> FlowKeys.root_index_key(value, partition_key)
        :correlation -> FlowKeys.correlation_index_key(value, partition_key)
      end

    [{key, id, score} | entries]
  end

  defp flow_metadata_index_entry(entries, _kind, _value, _partition_key, _id, _score), do: entries

  defp flow_metadata_index_entries_with_tag(record, tag) do
    id = Map.get(record, :id)
    parent_flow_id = Map.get(record, :parent_flow_id)
    root_flow_id = Map.get(record, :root_flow_id)
    correlation_id = Map.get(record, :correlation_id)

    if flow_metadata_index_empty?(parent_flow_id, root_flow_id, correlation_id, id) do
      []
    else
      score = Map.get(record, :updated_at_ms, 0)

      []
      |> flow_metadata_index_entry_with_tag(:parent, parent_flow_id, tag, id, score)
      |> flow_metadata_index_entry_with_tag(:root, root_flow_id, tag, id, score)
      |> flow_metadata_index_entry_with_tag(:correlation, correlation_id, tag, id, score)
    end
  end

  defp flow_metadata_index_entry_with_tag(entries, :root, nil, _tag, _id, _score), do: entries
  defp flow_metadata_index_entry_with_tag(entries, :root, "", _tag, _id, _score), do: entries
  defp flow_metadata_index_entry_with_tag(entries, :root, id, _tag, id, _score), do: entries

  defp flow_metadata_index_entry_with_tag(entries, kind, value, tag, id, score)
       when is_binary(value) and value != "" do
    key =
      case kind do
        :parent -> flow_parent_index_key_with_tag(tag, value)
        :root -> flow_root_index_key_with_tag(tag, value)
        :correlation -> flow_correlation_index_key_with_tag(tag, value)
      end

    [{key, id, score} | entries]
  end

  defp flow_metadata_index_entry_with_tag(entries, _kind, _value, _tag, _id, _score), do: entries

  defp flow_running_index_put(state, %{state: "running", id: id, type: type} = record) do
    partition_key = Map.get(record, :partition_key)
    lease_score = Map.get(record, :lease_deadline_ms, 0)
    inflight_index_key = FlowKeys.inflight_index_key(type, partition_key)
    worker_index_key = FlowKeys.worker_index_key(Map.get(record, :lease_owner, ""), partition_key)

    with :ok <-
           flow_index_put_new_lifecycle_members(state, inflight_index_key, [{id, lease_score}]) do
      flow_index_put_new_lifecycle_members(state, worker_index_key, [{id, lease_score}])
    end
  end

  defp flow_running_index_put(_state, _record), do: :ok

  defp flow_due_put_many_new(_state, []), do: :ok

  defp flow_due_put_many_new(state, records) do
    flow_due_put_many_with(state, records, &flow_index_put_new_lifecycle_members/3)
  end

  defp flow_due_put_many_with(state, records, put_fun) do
    records
    |> Enum.flat_map(fn record ->
      partition_key = Map.get(record, :partition_key)

      entries = [
        {FlowKeys.due_key(record.type, record.state, record.priority, partition_key), record}
      ]

      if flow_due_any_index_enabled?() do
        [{FlowKeys.due_any_key(record.type, record.priority, partition_key), record} | entries]
      else
        entries
      end
    end)
    |> Enum.group_by(
      fn {due_key, _record} ->
        due_key
      end,
      fn {_due_key, record} ->
        record
      end
    )
    |> Enum.each(fn {due_key, due_records} ->
      member_score_pairs =
        Enum.map(due_records, fn record ->
          {record.id, Map.fetch!(record, :next_run_at_ms)}
        end)

      put_fun.(state, due_key, member_score_pairs)
    end)

    :ok
  end

  defp flow_ensure_due_index_ready(
         %{zset_score_lookup_name: lookup, zset_score_index_name: index},
         due_key
       )
       when lookup != nil and index != nil do
    if :ets.whereis(lookup) != :undefined and :ets.whereis(index) != :undefined do
      :ets.insert_new(lookup, {{:count, due_key}, 0})
      :ets.insert(lookup, {{:ready, due_key}, true})
    end

    :ok
  end

  defp flow_index_tables?(%{flow_lookup_name: lookup, flow_index_name: index})
       when lookup != nil and index != nil do
    :ets.whereis(lookup) != :undefined and :ets.whereis(index) != :undefined
  end

  defp flow_index_tables?(_state), do: false

  defp flow_native_index(%{flow_index_name: index, flow_lookup_name: lookup})
       when index != nil and lookup != nil do
    NativeFlowIndex.get(index, lookup)
  end

  defp flow_native_index(_state), do: nil

  defp flow_claim_index_count_keys(state) do
    case flow_native_index(state) do
      nil -> flow_index_count_keys(state)
      native -> NativeFlowIndex.due_count_keys(native)
    end
  end

  defp flow_native_index_for_read(state, key) do
    if flow_native_lifecycle_index_key?(key) and
         Process.get(:sm_pending_flow_native_ops, []) == [] do
      flow_native_index(state)
    end
  end

  defp flow_native_lifecycle_index_key?(key) when is_binary(key) do
    :binary.match(key, "}:d:") != :nomatch or
      :binary.match(key, "}:da:") != :nomatch or
      :binary.match(key, "}:i:s:") != :nomatch or
      :binary.match(key, "}:i:r:") != :nomatch or
      :binary.match(key, "}:i:w:") != :nomatch
  end

  defp flow_native_lifecycle_index_key?(_key), do: false

  defp flow_index_rank_range(
         %{flow_index_name: index} = state,
         key,
         start_idx,
         stop_idx,
         reverse?
       ) do
    case flow_native_index_for_read(state, key) do
      nil ->
        if flow_index_tables?(state) do
          FlowIndex.rank_range(index, key, start_idx, stop_idx, reverse?)
        else
          []
        end

      native ->
        NativeFlowIndex.rank_range(native, key, start_idx, stop_idx, reverse?)
    end
  end

  defp flow_index_rank_range(_state, _key, _start_idx, _stop_idx, _reverse?), do: []

  defp flow_index_count_all(%{flow_lookup_name: lookup} = state, key) do
    case flow_native_index_for_read(state, key) do
      nil -> if flow_index_tables?(state), do: FlowIndex.count_all(lookup, key), else: 0
      native -> NativeFlowIndex.count_all(native, key)
    end
  end

  defp flow_index_count_all(_state, _key), do: 0

  defp flow_index_count_keys(%{flow_lookup_name: lookup} = state) do
    case flow_native_index(state) do
      nil -> if flow_index_tables?(state), do: FlowIndex.due_count_keys(lookup), else: []
      native -> NativeFlowIndex.due_count_keys(native)
    end
  end

  defp flow_index_count_keys(_state), do: []

  defp flow_index_score_of(%{flow_lookup_name: lookup} = state, key, member) do
    case flow_native_index_for_read(state, key) do
      nil ->
        if flow_index_tables?(state), do: FlowIndex.score_of(lookup, key, member), else: :miss

      native ->
        NativeFlowIndex.score_of(native, key, member)
    end
  end

  defp flow_index_score_of(_state, _key, _member), do: :miss

  defp flow_zset_put_many_new(
         state,
         due_key,
         member_score_pairs
       ) do
    flow_index_put_new_members(state, due_key, member_score_pairs)
  end

  defp flow_zset_delete_many(
         state,
         due_key,
         ids
       ) do
    flow_index_delete_members(state, due_key, ids)
  end

  defp flow_index_put_members(state, key, member_score_pairs) do
    if flow_index_tables?(state) do
      track_flow_index_originals(
        state,
        key,
        Enum.map(member_score_pairs, fn {member, _score} -> member end)
      )

      FlowIndex.put_members(
        state.flow_index_name,
        state.flow_lookup_name,
        key,
        member_score_pairs
      )

      flow_native_put_members(state, key, member_score_pairs)
    end

    :ok
  end

  defp flow_index_put_new_members(state, key, member_score_pairs) do
    if flow_index_tables?(state) do
      track_flow_index_missing_originals(
        state,
        key,
        Enum.map(member_score_pairs, fn {member, _score} -> member end)
      )

      FlowIndex.put_new_members(
        state.flow_index_name,
        state.flow_lookup_name,
        key,
        member_score_pairs
      )

      flow_native_put_new_members(state, key, member_score_pairs)
    end

    :ok
  end

  defp flow_index_put_new_lifecycle_members(state, key, member_score_pairs) do
    if flow_native_lifecycle_pending?(state) do
      flow_native_put_new_members(state, key, member_score_pairs)
    else
      if flow_index_tables?(state) do
        track_flow_index_missing_originals(
          state,
          key,
          Enum.map(member_score_pairs, fn {member, _score} -> member end),
          :lifecycle
        )

        FlowIndex.put_new_members(
          state.flow_index_name,
          state.flow_lookup_name,
          key,
          member_score_pairs
        )

        flow_native_put_new_members(state, key, member_score_pairs)
      end
    end

    :ok
  end

  defp flow_native_lifecycle_pending?(state) do
    case flow_native_index(state) do
      nil -> false
      _native -> is_list(Process.get(:sm_pending_flow_native_ops, :undefined))
    end
  end

  defp flow_index_put_new_entries(state, key_member_score_triples) do
    if flow_index_tables?(state) do
      key_member_score_triples
      |> Enum.group_by(fn {key, _member, _score} -> key end, fn {_key, member, _score} ->
        member
      end)
      |> Enum.each(fn {key, members} ->
        track_flow_index_missing_originals(state, key, members)
      end)

      FlowIndex.put_new_entries(
        state.flow_index_name,
        state.flow_lookup_name,
        key_member_score_triples
      )

      flow_native_put_new_entries(state, key_member_score_triples)
    end

    :ok
  end

  defp flow_index_move_entries(state, key_key_member_score_quads) do
    if flow_index_tables?(state) do
      key_key_member_score_quads
      |> Enum.flat_map(fn {from_key, to_key, member, _score} ->
        [{from_key, member}, {to_key, member}]
      end)
      |> Enum.group_by(fn {key, _member} -> key end, fn {_key, member} -> member end)
      |> Enum.each(fn {key, members} ->
        track_flow_index_originals(state, key, Enum.uniq(members))
      end)

      FlowIndex.move_entries(
        state.flow_index_name,
        state.flow_lookup_name,
        key_key_member_score_quads
      )

      flow_native_move_entries(state, key_key_member_score_quads)
    end

    :ok
  end

  defp flow_index_move_lifecycle_entries(state, [{_from_key, _to_key, _member, _score} = entry]) do
    if flow_native_lifecycle_pending?(state) do
      flow_native_move_entries(state, [entry])
    else
      flow_index_move_lifecycle_entries_ets(state, [entry])
    end
  end

  defp flow_index_move_lifecycle_entries(state, key_key_member_score_quads) do
    if flow_native_lifecycle_pending?(state) do
      flow_native_move_entries(state, key_key_member_score_quads)
    else
      flow_index_move_lifecycle_entries_ets(state, key_key_member_score_quads)
    end
  end

  defp flow_index_move_lifecycle_entries_ets(state, [{from_key, to_key, member, _score} = entry]) do
    if flow_index_tables?(state) do
      if from_key == to_key do
        track_flow_index_originals(state, from_key, [member], :lifecycle)
      else
        track_flow_index_originals(state, from_key, [member], :lifecycle)
        track_flow_index_originals(state, to_key, [member], :lifecycle)
      end

      FlowIndex.move_entries(state.flow_index_name, state.flow_lookup_name, [entry])
      flow_native_move_entries(state, [entry])
    end

    :ok
  end

  defp flow_index_move_lifecycle_entries_ets(state, key_key_member_score_quads) do
    if flow_index_tables?(state) do
      key_key_member_score_quads
      |> Enum.flat_map(fn {from_key, to_key, member, _score} ->
        [{from_key, member}, {to_key, member}]
      end)
      |> Enum.group_by(fn {key, _member} -> key end, fn {_key, member} -> member end)
      |> Enum.each(fn {key, members} ->
        track_flow_index_originals(state, key, Enum.uniq(members), :lifecycle)
      end)

      FlowIndex.move_entries(
        state.flow_index_name,
        state.flow_lookup_name,
        key_key_member_score_quads
      )

      flow_native_move_entries(state, key_key_member_score_quads)
    end

    :ok
  end

  defp flow_index_delete_members(state, key, members) do
    if flow_index_tables?(state) do
      track_flow_index_originals(state, key, members)
      FlowIndex.delete_members(state.flow_index_name, state.flow_lookup_name, key, members)
      flow_native_delete_members(state, key, members)
    end

    :ok
  end

  defp flow_index_delete_lifecycle_members(state, key, members) do
    if flow_native_lifecycle_pending?(state) do
      flow_native_delete_members(state, key, members)
    else
      if flow_index_tables?(state) do
        track_flow_index_originals(state, key, members, :lifecycle)
        FlowIndex.delete_members(state.flow_index_name, state.flow_lookup_name, key, members)
        flow_native_delete_members(state, key, members)
      end
    end

    :ok
  end

  defp flow_native_put_members(state, key, member_score_pairs) do
    case flow_native_index(state) do
      nil ->
        :ok

      native ->
        entries = Enum.map(member_score_pairs, fn {member, score} -> {key, member, score} end)
        flow_native_apply_or_queue(native, {:put_entries, entries})
    end
  end

  defp flow_native_put_new_members(state, key, member_score_pairs) do
    case flow_native_index(state) do
      nil ->
        :ok

      native ->
        entries = Enum.map(member_score_pairs, fn {member, score} -> {key, member, score} end)
        flow_native_apply_or_queue(native, {:put_new_entries, entries})
    end
  end

  defp flow_native_put_new_entries(state, key_member_score_triples) do
    case flow_native_index(state) do
      nil -> :ok
      native -> flow_native_apply_or_queue(native, {:put_new_entries, key_member_score_triples})
    end
  end

  defp flow_native_move_entries(state, key_key_member_score_quads) do
    case flow_native_index(state) do
      nil -> :ok
      native -> flow_native_apply_or_queue(native, {:move_entries, key_key_member_score_quads})
    end
  end

  defp flow_native_delete_members(state, key, members) do
    case flow_native_index(state) do
      nil -> :ok
      native -> flow_native_apply_or_queue(native, {:delete_members, key, members})
    end
  end

  defp flow_native_apply_claim_entries(state, entries) do
    case flow_native_index(state) do
      nil -> :ok
      native -> flow_native_apply_or_queue(native, {:apply_claim_entries, entries})
    end
  end

  defp flow_native_apply_or_queue(native, op) do
    case Process.get(:sm_pending_flow_native_ops, :undefined) do
      ops when is_list(ops) ->
        Process.put(:sm_pending_flow_native_ops, [{native, op} | ops])

      _ ->
        NativeFlowIndex.apply_batch(native, [op])
    end

    :ok
  end

  defp track_flow_index_missing_originals(state, key, members) do
    track_flow_index_missing_originals(state, key, members, :auto)
  end

  defp track_flow_index_missing_originals(state, key, members, native_hint) do
    case Process.get(:sm_pending_flow_index_originals, :undefined) do
      originals when is_map(originals) ->
        track_flow_index_count_original(state, key, native_hint)

        originals =
          Enum.reduce(members, originals, fn member, acc ->
            original_key = {state.flow_index_name, state.flow_lookup_name, key, member}

            if Map.has_key?(acc, original_key) do
              acc
            else
              original =
                case flow_index_member_original(state, key, member, native_hint) do
                  {:score, _score} = original -> original
                  :missing -> :missing
                end

              Map.put(acc, original_key, original)
            end
          end)

        Process.put(:sm_pending_flow_index_originals, originals)

      _ ->
        :ok
    end

    :ok
  end

  defp track_flow_index_originals(state, key, members) do
    track_flow_index_originals(state, key, members, :auto)
  end

  defp track_flow_index_originals(state, key, members, native_hint) do
    case Process.get(:sm_pending_flow_index_originals, :undefined) do
      originals when is_map(originals) ->
        track_flow_index_count_original(state, key, native_hint)

        originals =
          Enum.reduce(members, originals, fn member, acc ->
            original_key = {state.flow_index_name, state.flow_lookup_name, key, member}

            if Map.has_key?(acc, original_key) do
              acc
            else
              original = flow_index_member_original(state, key, member, native_hint)

              Map.put(acc, original_key, original)
            end
          end)

        Process.put(:sm_pending_flow_index_originals, originals)

      _ ->
        :ok
    end

    :ok
  end

  defp track_flow_index_count_original(state, key) do
    track_flow_index_count_original(state, key, :auto)
  end

  defp track_flow_index_count_original(state, key, native_hint) do
    case Process.get(:sm_pending_flow_index_counts, :undefined) do
      counts when is_map(counts) ->
        count_key = {state.flow_index_name, state.flow_lookup_name, key}

        if Map.has_key?(counts, count_key) do
          :ok
        else
          original = flow_index_count_original(state, key, native_hint)

          Process.put(:sm_pending_flow_index_counts, Map.put(counts, count_key, original))
        end

      _ ->
        :ok
    end

    :ok
  end

  defp flow_index_member_original(state, key, member, :lifecycle) do
    case flow_native_index(state) do
      nil ->
        flow_index_ets_member_original(state, key, member)

      native ->
        case NativeFlowIndex.score_of(native, key, member) do
          {:ok, score} -> {:score, score}
          :miss -> :missing
        end
    end
  end

  defp flow_index_member_original(state, key, member, :auto) do
    case flow_native_index_for_read(state, key) do
      nil ->
        flow_index_ets_member_original(state, key, member)

      native ->
        case NativeFlowIndex.score_of(native, key, member) do
          {:ok, score} -> {:score, score}
          :miss -> :missing
        end
    end
  end

  defp flow_index_ets_member_original(state, key, member) do
    case FlowIndex.score_of(state.flow_lookup_name, key, member) do
      {:ok, score} -> {:score, score}
      :miss -> :missing
    end
  end

  defp flow_index_count_original(state, key, :lifecycle) do
    case flow_native_index(state) do
      nil ->
        flow_index_ets_count_original(state, key)

      native ->
        case NativeFlowIndex.count_all(native, key) do
          count when is_integer(count) and count > 0 -> {:count, count}
          _other -> :missing
        end
    end
  end

  defp flow_index_count_original(state, key, :auto) do
    case flow_native_index_for_read(state, key) do
      nil ->
        flow_index_ets_count_original(state, key)

      native ->
        case NativeFlowIndex.count_all(native, key) do
          count when is_integer(count) and count > 0 -> {:count, count}
          _other -> :missing
        end
    end
  end

  defp flow_index_ets_count_original(state, key) do
    case :ets.lookup(state.flow_lookup_name, {:count, key}) do
      [{{:count, ^key}, count}] when is_integer(count) -> {:count, count}
      _ -> :missing
    end
  end

  defp rollback_pending_flow_indexes do
    Process.get(:sm_pending_fast_claim_index_rollbacks, [])
    |> Enum.each(fn {index_table, lookup_table, entries} ->
      if :ets.whereis(index_table) != :undefined and :ets.whereis(lookup_table) != :undefined do
        rollback_fast_claim_index_entries(index_table, lookup_table, entries)
      end
    end)

    Process.get(:sm_pending_flow_index_originals, %{})
    |> Enum.each(fn
      {{index_table, lookup_table, key, member}, {:score, score}} ->
        if :ets.whereis(index_table) != :undefined and :ets.whereis(lookup_table) != :undefined do
          FlowIndex.put_member(index_table, lookup_table, key, member, score)
        end

      {{index_table, lookup_table, key, member}, :missing} ->
        if :ets.whereis(index_table) != :undefined and :ets.whereis(lookup_table) != :undefined do
          FlowIndex.delete_member(index_table, lookup_table, key, member)
        end
    end)

    Process.get(:sm_pending_flow_index_counts, %{})
    |> Enum.each(fn
      {{_index_table, lookup_table, key}, {:count, count}} ->
        if :ets.whereis(lookup_table) != :undefined do
          FlowIndex.restore_count(lookup_table, key, count)
        end

      {{_index_table, lookup_table, key}, :missing} ->
        if :ets.whereis(lookup_table) != :undefined do
          FlowIndex.delete_count(lookup_table, key)
        end
    end)

    :ok
  end

  defp rollback_fast_claim_index_entries(index_table, lookup_table, entries) do
    Enum.each(entries, fn {id, from_due_key, from_due_score, to_due_key, to_due_score,
                           from_state_key, from_state_score, to_state_key, to_state_score,
                           inflight_key, worker_key, lease_score} ->
      :ets.delete(index_table, {to_due_key, to_due_score, id})
      :ets.delete(index_table, {to_state_key, to_state_score, id})
      :ets.delete(index_table, {inflight_key, lease_score, id})
      :ets.delete(index_table, {worker_key, lease_score, id})

      :ets.delete(lookup_table, {to_due_key, id})
      :ets.delete(lookup_table, {to_state_key, id})
      :ets.delete(lookup_table, {inflight_key, id})
      :ets.delete(lookup_table, {worker_key, id})

      :ets.insert(index_table, [
        {{from_due_key, from_due_score, id}, true},
        {{from_state_key, from_state_score, id}, true}
      ])

      :ets.insert(lookup_table, [
        {{from_due_key, id}, from_due_score},
        {{from_state_key, id}, from_state_score}
      ])
    end)
  end

  defp flow_history_put(state, record, event, now_ms) do
    flow_history_put_ready(state, record, event, now_ms)
  end

  defp flow_history_put_planned(state, previous, record, event, now_ms) do
    flow_history_put_ready(state, record, event, now_ms, flow_previous_history_ms(previous))
  end

  defp flow_history_put_planned(state, previous, record, event, now_ms, meta) do
    flow_history_put_ready(state, record, event, now_ms, flow_previous_history_ms(previous), meta)
  end

  defp flow_history_put_ready(state, record, event, now_ms) do
    flow_history_put_ready(state, record, event, now_ms, nil)
  end

  defp flow_history_put_ready(state, record, event, now_ms, previous_history_ms) do
    flow_history_put_ready(state, record, event, now_ms, previous_history_ms, %{})
  end

  defp flow_history_put_ready(
         state,
         %{id: id, version: version} = record,
         event,
         now_ms,
         previous_history_ms,
         meta
       ) do
    partition_key = Map.get(record, :partition_key)
    history_key = FlowKeys.history_key(id, partition_key)

    {event_id, event_ms} =
      flow_history_next_event(state, history_key, now_ms, version, previous_history_ms)

    compound_key = FlowKeys.stream_entry_key_from_history_key(history_key, event_id)

    entry = %{
      key: compound_key,
      expire_at_ms: 0,
      history_key: history_key,
      event_id: event_id,
      event_ms: event_ms,
      version: version,
      history_max_events: Map.get(record, :history_max_events),
      record: record,
      event: event,
      now_ms: now_ms,
      meta: meta
    }

    with :ok <-
           flow_history_put_or_queue_entry(state, entry) do
      if flow_async_history_enabled?(state) do
        :ok
      else
        flow_history_index_put(state, history_key, event_id, event_ms, version)
      end
    end
  end

  defp flow_history_next_event(_state, _history_key, now_ms, 1, _previous_history_ms) do
    {Integer.to_string(trunc(now_ms)) <> "-1", trunc(now_ms)}
  end

  defp flow_history_next_event(_state, _history_key, now_ms, version, previous_history_ms)
       when is_integer(previous_history_ms) do
    ms = max(trunc(now_ms), previous_history_ms)
    {Integer.to_string(ms) <> "-" <> Integer.to_string(version), ms}
  end

  defp flow_history_next_event(state, history_key, now_ms, version, _previous_history_ms) do
    ms =
      case flow_index_rank_range(state, history_key, 0, 0, true) do
        [{_event_id, last_ms}] when is_number(last_ms) and last_ms > now_ms ->
          last_ms

        _ ->
          now_ms
      end

    {Integer.to_string(trunc(ms)) <> "-" <> Integer.to_string(version), trunc(ms)}
  end

  defp flow_previous_history_ms(%{updated_at_ms: updated_at_ms}) when is_integer(updated_at_ms),
    do: updated_at_ms

  defp flow_previous_history_ms(%{created_at_ms: created_at_ms}) when is_integer(created_at_ms),
    do: created_at_ms

  defp flow_previous_history_ms(_record), do: nil

  defp flow_history_put_or_queue_entry(state, entry) do
    if flow_async_history_enabled?(state) do
      queue_pending_flow_history_projection(entry)
    else
      raw_put_cold(state, entry.key, flow_history_entry_value(entry), entry.expire_at_ms)
    end
  end

  defp flow_history_entry_value(%{value: value}) when is_binary(value), do: value

  defp flow_history_entry_value(%{snapshot: snapshot}) do
    Flow.encode_history_snapshot(snapshot)
  end

  defp flow_history_entry_value(%{record: record, event: event, now_ms: now_ms} = entry) do
    Flow.encode_history_fields(record, event, now_ms, Map.get(entry, :meta, %{}))
  end

  defp flow_async_history?(state), do: Map.get(state, :flow_async_history, false) == true

  defp flow_async_history_enabled?(state) do
    flow_async_history?(state) or Process.get(@sm_force_async_flow_history_key) == true
  end

  defp flow_with_forced_async_history(fun) when is_function(fun, 0) do
    previous = Process.get(@sm_force_async_flow_history_key, :unset)
    Process.put(@sm_force_async_flow_history_key, true)

    try do
      fun.()
    after
      case previous do
        :unset -> Process.delete(@sm_force_async_flow_history_key)
        value -> Process.put(@sm_force_async_flow_history_key, value)
      end
    end
  end

  defp flow_history_index_put(state, history_key, event_id, ms, 1) do
    flow_index_put_new_members(state, history_key, [{event_id, ms}])
    :ok
  end

  defp flow_history_index_put(state, history_key, event_id, ms, _version) do
    flow_index_put_members(state, history_key, [{event_id, ms}])
    :ok
  end

  defp flow_history_index_put_entries(_state, []), do: :ok

  defp flow_history_index_put_entries(state, entries) do
    if flow_async_history_enabled?(state) do
      :ok
    else
      flow_index_put_new_entries(state, entries)
    end
  end

  defp flow_history_trim(_state, %{history_max_events: nil}), do: :ok
  defp flow_history_trim(_state, %{history_max_events: max}) when not is_integer(max), do: :ok

  defp flow_history_trim(_state, %{history_max_events: max, version: version})
       when is_integer(version) and version <= max,
       do: :ok

  defp flow_history_trim(state, %{id: id, history_max_events: max} = record) when max > 0 do
    partition_key = Map.get(record, :partition_key)
    history_key = FlowKeys.history_key(id, partition_key)

    case flow_index_count_all(state, history_key) do
      len when len > max ->
        delete_count = len - max
        flow_history_trim_oldest(state, record, id, partition_key, history_key, delete_count)

      _ ->
        :ok
    end
  end

  defp flow_history_trim(_state, _record), do: :ok

  defp flow_after_history_put_many(records, state) do
    Enum.reduce_while(records, :ok, fn record, :ok ->
      case flow_after_history_put(state, record) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp flow_after_history_put(state, record) do
    with :ok <- flow_history_trim(state, record) do
      maybe_queue_terminal_lmdb_history_indexes(state, record)
    end
  end

  defp flow_history_trim_oldest(state, record, id, partition_key, history_key, delete_count) do
    events = flow_index_rank_range(state, history_key, 0, delete_count - 1, false)

    with :ok <-
           flow_history_delete_oldest_events(
             state,
             record,
             id,
             partition_key,
             history_key,
             events
           ) do
      events
      |> Enum.map(fn {event_id, _event_ms} -> event_id end)
      |> then(&flow_index_delete_members(state, history_key, &1))
    end
  end

  defp flow_history_delete_oldest_events(_state, _record, _id, _partition_key, _history_key, []),
    do: :ok

  defp flow_history_delete_oldest_events(state, record, id, partition_key, history_key, events) do
    Enum.reduce_while(events, :ok, fn {event_id, event_ms}, :ok ->
      compound_key = FlowKeys.stream_entry_key(id, event_id, partition_key)

      if flow_lmdb_mirror?(state) do
        with_lmdb_mirror_shard(state, fn ->
          queue_lmdb_history_index_delete(record, history_key, event_id, trunc(event_ms))
        end)
      end

      case do_delete(state, compound_key) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp maybe_queue_terminal_lmdb_history_indexes(state, record) do
    if flow_lmdb_mirror?(state) and Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      id = Map.fetch!(record, :id)
      partition_key = Map.get(record, :partition_key)
      history_key = FlowKeys.history_key(id, partition_key)

      with_lmdb_mirror_shard(state, fn ->
        queue_lmdb_history_indexes_project_from_index(state, record, history_key)
      end)
    end

    :ok
  end

  defp flow_record_expire_at(%{terminal_retention_until_ms: expire_at_ms})
       when is_integer(expire_at_ms) and expire_at_ms > 0,
       do: expire_at_ms

  defp flow_record_expire_at(_record), do: 0

  defp flow_encode(record), do: Flow.encode_record(record)

  defp do_put(state, key, value, expire_at_ms) do
    maybe_clear_compound_data_structure_for_string_put(state, key)

    case maybe_externalize_apply_value(state, value) do
      {:ok, :value, value} ->
        raw_put(state, key, value, expire_at_ms)

      {:ok, :blob_ref, encoded_ref, materialized_value} ->
        raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value)

      {:error, _reason} = error ->
        error
    end
  end

  defp prepare_apply_blob_command(state, command) do
    ctx = blob_apply_ctx(state)

    if BlobCommand.side_channel_candidate?(ctx, command) do
      case BlobCommand.prepare(ctx, state.shard_index, command, single_member?: true) do
        {:ok, prepared_command} -> {:ok, prepared_command}
        {:error, reason} -> {:error, {:blob_externalize_failed, reason}}
      end
    else
      {:ok, command}
    end
  end

  defp blob_apply_ctx(%{instance_ctx: %{data_dir: data_dir} = ctx}) when is_binary(data_dir),
    do: ctx

  defp blob_apply_ctx(%{data_dir: data_dir, blob_side_channel_threshold_bytes: threshold})
       when is_binary(data_dir) do
    %{data_dir: data_dir, blob_side_channel_threshold_bytes: threshold}
  end

  defp blob_apply_ctx(_state), do: %{blob_side_channel_threshold_bytes: 0}

  defp maybe_externalize_apply_value(state, value) when is_binary(value) do
    ctx = blob_apply_ctx(state)
    threshold = BlobValue.threshold(ctx)

    case BlobValue.maybe_externalize(Map.get(ctx, :data_dir), state.shard_index, threshold, value) do
      {:ok, ^value} ->
        {:ok, :value, value}

      {:ok, encoded_ref} ->
        {:ok, :blob_ref, encoded_ref, value}

      {:error, reason} ->
        {:error, {:blob_externalize_failed, reason}}
    end
  end

  defp maybe_externalize_apply_value(_state, value), do: {:ok, :value, value}

  defp maybe_externalize_cross_shard_value(anchor_state, ctx, value) when is_binary(value) do
    instance_ctx = Map.get(ctx, :instance_ctx) || Map.get(anchor_state, :instance_ctx)
    threshold = BlobValue.threshold(instance_ctx)

    case BlobValue.maybe_externalize(ctx.data_dir, ctx.index, threshold, value) do
      {:ok, ^value} ->
        {:ok, value_for_ets(value, hot_cache_threshold(anchor_state)), to_disk_binary(value),
         value}

      {:ok, encoded_ref} ->
        {:ok, nil, to_disk_binary(encoded_ref), value}

      {:error, reason} ->
        {:error, {:blob_externalize_failed, reason}}
    end
  end

  defp maybe_externalize_cross_shard_value(anchor_state, _ctx, value) do
    {:ok, value_for_ets(value, hot_cache_threshold(anchor_state)), to_disk_binary(value), value}
  end

  defp flow_put_record_values(state, record, attrs) do
    do_flow_put_record_values(state, record, attrs)
  end

  defp do_flow_put_record_values(state, record, attrs) do
    with :ok <- flow_maybe_put_record_value(state, record, attrs, :payload),
         :ok <- flow_maybe_put_record_value(state, record, attrs, :result),
         :ok <- flow_maybe_put_record_value(state, record, attrs, :error) do
      flow_put_named_record_values(state, record, attrs)
    end
  end

  defp flow_maybe_put_record_value(state, record, attrs, kind) do
    if Map.has_key?(attrs, kind) do
      key = Map.fetch!(record, flow_value_ref_field(kind))

      with :ok <- flow_validate_key_size(key) do
        raw_put_cold(
          state,
          key,
          Flow.encode_value(Map.fetch!(attrs, kind)),
          flow_record_expire_at(record)
        )
      end
    else
      :ok
    end
  end

  defp flow_put_named_record_values(state, record, attrs) do
    values = flow_named_values(Map.get(attrs, :values))

    if map_size(values) == 0 do
      :ok
    else
      refs = flow_record_value_refs(record)

      Enum.reduce_while(values, :ok, fn {name, value}, :ok ->
        case Map.get(refs, name) do
          %{ref: key} when is_binary(key) and key != "" ->
            with :ok <- flow_validate_key_size(key),
                 :ok <-
                   raw_put_cold(
                     state,
                     key,
                     Flow.encode_value(value),
                     flow_record_expire_at(record)
                   ) do
              {:cont, :ok}
            else
              {:error, _reason} = error -> {:halt, error}
            end

          _missing ->
            {:halt, {:error, "ERR flow value #{name} missing ref"}}
        end
      end)
    end
  end

  defp flow_refresh_terminal_value_expirations(state, record, attrs) do
    flow_refresh_terminal_value_expirations_without_materializing(state, record, attrs)
  end

  defp flow_refresh_terminal_value_expirations_without_materializing(_state, _record, _attrs) do
    # Payload/result/error bytes are separate value/blob records. Terminal state
    # writes must not read those bytes just to refresh TTL: large payloads would
    # turn a metadata transition into a hidden cold-read/materialize path. Newly
    # supplied values are already written above with the terminal record expiry;
    # existing refs keep their original value-retention policy.
    :ok
  end

  defp flow_refresh_record_value_expirations(state, record, attrs) do
    refs =
      [:payload, :result, :error]
      |> Enum.reject(&Map.has_key?(attrs, &1))
      |> Enum.map(fn kind -> Map.get(record, flow_value_ref_field(kind)) end)
      |> Enum.filter(&flow_owned_value_ref?/1)

    values = sm_store_batch_get(state, refs, &sm_file_path/2)
    expire_at_ms = flow_record_expire_at(record)

    refs
    |> Enum.zip(values)
    |> Enum.reduce_while(:ok, fn
      {_key, nil}, :ok ->
        {:cont, :ok}

      {key, value}, :ok when is_binary(value) ->
        with :ok <- flow_validate_key_size(key),
             :ok <- raw_put_cold(state, key, value, expire_at_ms) do
          {:cont, :ok}
        else
          {:error, _reason} = error -> {:halt, error}
        end

      {_key, _value}, :ok ->
        {:cont, :ok}
    end)
  end

  defp flow_create_put_record_values(state, plans) do
    if flow_create_plans_have_record_values?(plans) do
      Enum.reduce_while(plans, :ok, fn {record, attrs}, :ok ->
        case flow_put_record_values(state, record, attrs) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    else
      :ok
    end
  end

  defp flow_many_put_record_values(state, plans) do
    flow_many_put_record_values(state, plans, :unknown)
  end

  defp flow_many_put_record_values(_state, _plans, false), do: :ok

  defp flow_many_put_record_values(state, plans, true) do
    flow_many_put_record_values_nonempty(state, plans)
  end

  defp flow_many_put_record_values(state, plans, :unknown) do
    if flow_many_plans_have_record_values?(plans) do
      flow_many_put_record_values_nonempty(state, plans)
    else
      :ok
    end
  end

  defp flow_many_put_record_values_nonempty(state, plans) do
    Enum.reduce_while(plans, :ok, fn
      {_record, next, attrs}, :ok ->
        case flow_put_record_values(state, next, attrs) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end

      {_record, next, _history_meta, attrs}, :ok ->
        case flow_put_record_values(state, next, attrs) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
    end)
  end

  defp flow_create_plans_have_record_values?(plans) do
    Enum.any?(plans, fn {_record, attrs} -> flow_attrs_have_record_values?(attrs) end)
  end

  defp flow_many_plans_have_record_values?(plans) do
    Enum.any?(plans, fn
      {_record, _next, attrs} -> flow_attrs_have_record_values?(attrs)
      {_record, _next, _history_meta, attrs} -> flow_attrs_have_record_values?(attrs)
    end)
  end

  defp flow_attrs_have_record_values?(attrs) do
    Map.has_key?(attrs, :payload) or Map.has_key?(attrs, :result) or Map.has_key?(attrs, :error) or
      map_size(flow_named_values(Map.get(attrs, :values))) > 0
  end

  defp flow_attrs_record_value_mode(attrs) do
    has_payload? = Map.has_key?(attrs, :payload)
    has_result? = Map.has_key?(attrs, :result)
    has_error? = Map.has_key?(attrs, :error)
    has_named? = map_size(flow_named_values(Map.get(attrs, :values))) > 0

    cond do
      has_payload? and not has_result? and not has_error? and not has_named? -> :payload_only
      has_payload? or has_result? or has_error? or has_named? -> :mixed
      true -> :none
    end
  end

  defp flow_merge_record_value_mode(:mixed, _mode), do: :mixed
  defp flow_merge_record_value_mode(_mode, :mixed), do: :mixed
  defp flow_merge_record_value_mode(:empty, mode), do: mode
  defp flow_merge_record_value_mode(:none, :none), do: :none
  defp flow_merge_record_value_mode(:none, :payload_only), do: :mixed
  defp flow_merge_record_value_mode(:payload_only, :none), do: :mixed
  defp flow_merge_record_value_mode(:none, mode), do: mode
  defp flow_merge_record_value_mode(mode, :none), do: mode
  defp flow_merge_record_value_mode(:payload_only, :payload_only), do: :payload_only

  defp flow_finalize_record_value_mode(:empty), do: :none
  defp flow_finalize_record_value_mode(mode), do: mode

  defp flow_put_state_record(state, key, record) when is_map(record) do
    flow_put_state_record_encoded(
      state,
      key,
      flow_encode(record),
      flow_record_expire_at(record),
      record
    )
  end

  defp flow_put_new_state_record(state, key, record) when is_map(record) do
    flow_put_state_record_encoded(
      state,
      key,
      flow_encode(record),
      flow_record_expire_at(record),
      record
    )
  end

  defp flow_put_state_record_encoded(state, key, value, expire_at_ms, record) do
    cond do
      flow_lmdb_mirror?(state) ->
        flow_mirror_put_state_record(state, key, value, expire_at_ms, record)
        maybe_queue_lmdb_indexes_for_state_record(state, key, value, expire_at_ms, record)

      true ->
        flow_put_hot(state, key, value, expire_at_ms)
    end
  end

  defp flow_mirror_put_state_record(state, key, value, expire_at_ms, record) do
    if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      raw_put_cold(state, key, value, expire_at_ms, flow_record_lfu(record, value))
    else
      flow_put_hot(state, key, value, expire_at_ms)
    end
  end

  defp flow_put_hot(state, key, value, expire_at_ms) do
    case maybe_externalize_apply_value(state, value) do
      {:ok, :value, value} ->
        flow_put_hot_value(state, key, value, expire_at_ms)

      {:ok, :blob_ref, encoded_ref, materialized_value} ->
        raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value)

      {:error, _reason} = error ->
        error
    end
  end

  defp flow_put_hot_value(state, key, value, expire_at_ms) do
    disk_val = to_disk_binary(value)

    if cross_shard_pending_active?() do
      cross_shard_raw_put(state, key, value, disk_val, expire_at_ms, LFU.initial())
      maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
      :ok
    else
      record_pending_original(state, key)

      unless standalone_staged_apply?() do
        track_keydir_binary_delta(state, key, value, expire_at_ms)

        :ets.insert(
          state.ets,
          {key, value, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
        )
      end

      queue_pending_put(key, disk_val, expire_at_ms)
      Process.put(:sm_pending_fast_staged_put_batch, true)
      maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)

      :ok
    end
  end

  defp raw_put_cold(state, key, value, expire_at_ms) do
    raw_put_cold(state, key, value, expire_at_ms, flow_cold_lfu(value))
  end

  defp raw_put_cold(state, key, value, expire_at_ms, lfu) do
    case maybe_externalize_apply_value(state, value) do
      {:ok, :value, value} ->
        raw_put_cold_value(state, key, value, expire_at_ms, lfu)

      {:ok, :blob_ref, encoded_ref, materialized_value} ->
        raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value, lfu)

      {:error, _reason} = error ->
        error
    end
  end

  defp raw_put_cold_value(state, key, value, expire_at_ms, lfu) do
    disk_val = to_disk_binary(value)

    if cross_shard_pending_active?() do
      ets_val = nil
      cross_shard_raw_put(state, key, ets_val, disk_val, expire_at_ms, lfu)
    else
      record_pending_original(state, key)

      unless standalone_staged_apply?() do
        track_keydir_binary_delta(state, key, nil, expire_at_ms)

        :ets.insert(
          state.ets,
          {key, nil, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
        )
      end

      queue_pending_put_cold(key, disk_val, expire_at_ms, lfu)
      Process.put(:sm_pending_fast_staged_put_batch, true)
      :ok
    end
  end

  defp cross_shard_raw_put(state, key, ets_val, disk_val, expire_at_ms, lfu) do
    ctx = cross_shard_pending_ctx(state)
    record_cross_shard_pending_original(ctx, key)

    track_keydir_binary_delta_for_keydir(state, ctx.keydir, ctx.index, key, ets_val, expire_at_ms)

    :ets.insert(
      ctx.keydir,
      {key, ets_val, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
    )

    queue_cross_shard_pending_put(ctx, key, disk_val, expire_at_ms, ets_val)
    :ok
  end

  defp cross_shard_pending_ctx(state) do
    %{
      keydir: state.ets,
      index: state.shard_index,
      active_file_path: state.active_file_path,
      active_file_id: state.active_file_id
    }
  end

  defp cross_shard_pending_active? do
    is_list(Process.get(:sm_cross_shard_pending_writes, :undefined))
  end

  defp track_keydir_binary_delta_for_keydir(
         state,
         keydir,
         shard_index,
         key,
         new_value,
         new_expire_at_ms
       ) do
    ref = keydir_binary_ref(state)
    previous = :ets.lookup(keydir, key)

    ExpiryTracker.adjust(
      expiry_instance_ctx(state),
      shard_index,
      ExpiryTracker.entry_expire_at(previous),
      new_expire_at_ms
    )

    if ref do
      new_bytes = binary_byte_size(key) + binary_byte_size(new_value)

      old_bytes =
        case previous do
          [{^key, old_val, _, _, _, _, _}] -> binary_byte_size(key) + binary_byte_size(old_val)
          _ -> 0
        end

      delta = new_bytes - old_bytes
      if delta != 0, do: :atomics.add(ref, shard_index + 1, delta)
    end
  end

  defp flow_record_lfu(%{version: version}, _value) when is_integer(version) do
    {:flow_state_version, version, LFU.initial()}
  end

  defp flow_record_lfu(_record, value), do: flow_cold_lfu(value)

  defp flow_cold_lfu(value) when is_binary(value) do
    if Flow.record_blob?(value) do
      case flow_decode_record_blob(value) do
        {:ok, %{version: version}} when is_integer(version) ->
          {:flow_state_version, version, LFU.initial()}

        _ ->
          LFU.initial()
      end
    else
      LFU.initial()
    end
  end

  defp flow_cold_lfu(_value), do: LFU.initial()

  defp raw_put(state, key, value, expire_at_ms) do
    ets_val = value_for_ets(value, hot_cache_threshold(state))
    disk_val = to_disk_binary(value)

    if cross_shard_pending_active?() do
      cross_shard_raw_put(state, key, ets_val, disk_val, expire_at_ms, LFU.initial())
      maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
      :ok
    else
      record_pending_original(state, key)

      unless standalone_staged_apply?() do
        # Track binary memory: subtract old entry's bytes, add new entry's bytes.
        # This gives MemoryGuard accurate off-heap binary accounting.
        track_keydir_binary_delta(state, key, ets_val, expire_at_ms)

        # Insert into ETS immediately so subsequent read-modify-write commands
        # (INCR, APPEND, etc.) in the same batch see the correct value.
        # The file_id is :pending — flush_pending_writes will update it with
        # the real offset after the batch NIF call.
        :ets.insert(
          state.ets,
          {key, ets_val, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
        )
      end

      # Accumulate for batch disk write — flushed by flush_pending_writes
      # at the end of apply/3 before returning to ra.
      queue_pending_put(key, disk_val, expire_at_ms)
      maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)

      :ok
    end
  end

  defp do_set(state, key, value, expire_at_ms, opts) do
    compound_data_structure? = compound_data_structure_key?(state, key)
    get? = Map.get(opts, :get, false)
    current = set_current_meta(state, key, get?)
    exists? = current != nil or compound_data_structure?

    {old_value, old_expire_at_ms} =
      case current do
        nil -> {nil, expire_at_ms}
        {old_value, old_expire_at_ms} -> {old_value, old_expire_at_ms}
      end

    skip? =
      cond do
        Map.get(opts, :nx, false) and exists? -> true
        Map.get(opts, :xx, false) and not exists? -> true
        true -> false
      end

    cond do
      compound_data_structure? and get? ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      skip? and get? ->
        old_value

      skip? ->
        nil

      true ->
        effective_expire_at_ms =
          if Map.get(opts, :keepttl, false) and exists? do
            old_expire_at_ms
          else
            expire_at_ms
          end

        do_put(state, key, value, effective_expire_at_ms)
        if get?, do: old_value, else: :ok
    end
  end

  defp set_current_meta(state, key, true), do: do_get_meta(state, key)

  defp set_current_meta(state, key, false) do
    case plain_expire_at_ms(state, key) do
      nil -> nil
      expire_at_ms -> {nil, expire_at_ms}
    end
  end

  defp plain_expire_at_ms(state, key) do
    now = apply_now_ms()

    case :ets.lookup(state.ets, key) do
      [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
        0

      [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        0

      [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        exp

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when exp > now and valid_cold_location(fid, off, vsize) ->
        exp

      [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
        track_keydir_binary_remove_known(state, key, value)
        :ets.delete(state.ets, key)
        nil

      [] ->
        nil
    end
  end

  defp do_checked_put_blob_ref(state, key, encoded_ref, expire_at_ms) do
    redis_key = CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_put_blob_ref(state, key, encoded_ref, expire_at_ms)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp do_put_blob_ref(state, key, encoded_ref, expire_at_ms) do
    with {:ok, materialized} <- materialize_blob_ref(state, encoded_ref) do
      maybe_clear_compound_data_structure_for_string_put(state, key)
      raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized)
    end
  end

  defp do_checked_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts) do
    redis_key = CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp do_set_blob_ref(state, key, encoded_ref, expire_at_ms, opts) when is_map(opts) do
    compound_data_structure? = compound_data_structure_key?(state, key)
    get? = Map.get(opts, :get, false)
    current = set_current_meta(state, key, get?)
    exists? = current != nil or compound_data_structure?

    {old_value, old_expire_at_ms} =
      case current do
        nil -> {nil, expire_at_ms}
        {old_value, old_expire_at_ms} -> {old_value, old_expire_at_ms}
      end

    skip? =
      cond do
        Map.get(opts, :nx, false) and exists? -> true
        Map.get(opts, :xx, false) and not exists? -> true
        true -> false
      end

    cond do
      compound_data_structure? and get? ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}

      skip? and get? ->
        old_value

      skip? ->
        nil

      true ->
        effective_expire_at_ms =
          if Map.get(opts, :keepttl, false) and exists? do
            old_expire_at_ms
          else
            expire_at_ms
          end

        case do_put_blob_ref(state, key, encoded_ref, effective_expire_at_ms) do
          :ok -> if get?, do: old_value, else: :ok
          {:error, _reason} = error -> error
        end
    end
  end

  defp do_getset_blob_ref(state, key, encoded_ref) do
    with :ok <- ensure_string_key(state, key),
         {:ok, materialized} <- materialize_blob_ref(state, encoded_ref) do
      old = do_get(state, key)
      raw_put_blob_ref(state, key, encoded_ref, 0, materialized)
      old
    end
  end

  defp do_append_blob_ref(state, key, encoded_ref) do
    with {:ok, suffix} <- materialize_blob_ref(state, encoded_ref) do
      do_append(state, key, suffix)
    end
  end

  defp do_setrange_blob_ref(state, key, offset, encoded_ref) do
    with {:ok, value} <- materialize_blob_ref(state, encoded_ref) do
      do_setrange(state, key, offset, value)
    end
  end

  defp do_cas_blob_ref(state, key, expected, encoded_ref, expire_at_ms) do
    case ets_lookup(state, key) do
      {:hit, ^expected, old_exp} ->
        with {:ok, materialized} <- materialize_blob_ref(state, encoded_ref) do
          expire = if expire_at_ms, do: expire_at_ms, else: old_exp
          raw_put_blob_ref(state, key, encoded_ref, expire, materialized)
          1
        end

      {:hit, _other, _exp} ->
        0

      :expired ->
        nil

      :miss ->
        nil
    end
  end

  defp do_locked_put_blob_ref(state, key, encoded_ref, expire_at_ms, owner_ref) do
    redis_key = CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, owner_ref) do
      :ok -> do_put_blob_ref(state, key, encoded_ref, expire_at_ms)
      {:error, _reason} = error -> error
    end
  end

  defp do_checked_compound_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms) do
    redis_key = CompoundKey.extract_redis_key(compound_key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_compound_put_blob_ref(state, redis_key, compound_key, encoded_ref, expire_at_ms)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp do_compound_put_blob_ref(state, redis_key, compound_key, encoded_ref, expire_at_ms) do
    with {:ok, materialized} <- materialize_blob_ref(state, encoded_ref) do
      result =
        case promoted_compound_path(state, redis_key, compound_key) do
          nil ->
            raw_put_blob_ref(state, compound_key, encoded_ref, expire_at_ms, materialized)

          dedicated_path ->
            do_promoted_compound_put(
              state,
              redis_key,
              compound_key,
              encoded_ref,
              expire_at_ms,
              dedicated_path
            )
        end

      if result == :ok do
        zset_index_put(state, redis_key, compound_key, materialized)
      end

      result
    end
  end

  defp raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value) do
    raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value, LFU.initial())
  end

  defp raw_put_blob_ref(state, key, encoded_ref, expire_at_ms, materialized_value, lfu) do
    disk_val = to_disk_binary(encoded_ref)

    if cross_shard_pending_active?() do
      cross_shard_raw_put(state, key, nil, disk_val, expire_at_ms, lfu)
      put_pending_value(key, materialized_value, expire_at_ms)
      maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
      :ok
    else
      record_pending_original(state, key)

      unless standalone_staged_apply?() do
        track_keydir_binary_delta(state, key, nil, expire_at_ms)

        :ets.insert(
          state.ets,
          {key, nil, expire_at_ms, lfu, :pending, 0, byte_size(disk_val)}
        )
      end

      queue_pending_put_cold(key, disk_val, expire_at_ms, lfu)
      put_pending_value(key, materialized_value, expire_at_ms)
      Process.put(:sm_pending_fast_staged_put_batch, true)
      maybe_queue_lmdb_policy_put(key, disk_val, expire_at_ms)
      :ok
    end
  end

  defp put_pending_value(key, value, expire_at_ms) do
    pending_values = Process.get(:sm_pending_values, %{})
    Process.put(:sm_pending_values, Map.put(pending_values, key, {value, expire_at_ms}))
  end

  defp materialize_blob_ref(state, encoded_ref) when is_binary(encoded_ref) do
    case BlobRef.decode(encoded_ref) do
      {:ok, ref} ->
        case BlobStore.get(state.data_dir, state.shard_index, ref) do
          {:ok, value} -> {:ok, value}
          {:error, reason} -> {:error, {:blob_ref_unavailable, reason}}
        end

      :error ->
        {:error, {:blob_ref_unavailable, :invalid_blob_ref}}
    end
  end

  defp materialize_blob_ref(_state, _encoded_ref),
    do: {:error, {:blob_ref_unavailable, :invalid_blob_ref}}

  # Flushes all accumulated disk writes in a single NIF call, then updates
  # ETS entries with real file_id/offset. Called at the end of every apply/3
  # — no :pending entries remain after this returns.
  defp flush_pending_writes(state) do
    :ok = flush_pending_lmdb(state)

    case Process.put(:sm_pending_writes, []) do
      [] ->
        flush_pending_flow_native_indexes(state)

      pending when is_list(pending) ->
        batch = Enum.reverse(pending)
        {batch_bytes, record_bytes, delete_count} = bitcask_batch_stats(batch)

        case resolve_active_file(state) do
          :stale ->
            emit_bitcask_append_telemetry(
              state,
              System.monotonic_time(),
              length(batch),
              batch_bytes,
              delete_count,
              :stale
            )

            set_disk_pressure(state)
            rollback_pending_writes(state)
            {:error, :active_file_unavailable}

          {file_path, file_id} ->
            started_at = System.monotonic_time()
            append_result = append_pending_batch(file_path, batch)
            validated_append_result = validate_append_result(batch, append_result)

            emit_bitcask_append_telemetry(
              state,
              started_at,
              length(batch),
              batch_bytes,
              delete_count,
              validated_append_result
            )

            case validated_append_result do
              {:ok, locations} ->
                clear_disk_pressure(state)
                apply_pending_locations(state, file_id, batch, locations)
                flush_pending_flow_native_indexes(state)
                flush_pending_zset_indexes(state)
                observe_pending_lmdb_mirror_enqueue(state, enqueue_pending_lmdb_mirror(state))
                state = track_bitcask_append_bytes(state, file_path, file_id, record_bytes)
                apply_state_put(:pending_state, state)
                :ok

              {:error, reason} ->
                set_disk_pressure(state)
                rollback_pending_writes(state)
                {:error, {:bitcask_append_failed, reason}}
            end
        end

      _ ->
        :ok
    end
  end

  defp flush_pending_flow_native_indexes(state) do
    case Process.put(:sm_pending_flow_native_ops, []) do
      [] ->
        :ok

      ops when is_list(ops) ->
        ops
        |> Enum.reverse()
        |> Enum.reduce(%{}, fn
          {native, op}, acc ->
            Map.update(acc, native, [op], &[op | &1])

          op, acc ->
            case flow_native_index(state) do
              nil -> acc
              native -> Map.update(acc, native, [op], &[op | &1])
            end
        end)
        |> Enum.each(fn {native, native_ops} ->
          NativeFlowIndex.apply_batch(native, Enum.reverse(native_ops))
        end)

        :ok

      _ ->
        :ok
    end
  end

  defp bitcask_batch_stats(batch) do
    Enum.reduce(batch, {0, 0, 0}, fn
      {:put, key, value, _expire_at_ms}, {batch_bytes, record_bytes, delete_count} ->
        bytes = byte_size(key) + byte_size(value)
        {batch_bytes + bytes, record_bytes + @bitcask_record_header_size + bytes, delete_count}

      {:put_cold, key, value, _expire_at_ms, _lfu}, {batch_bytes, record_bytes, delete_count} ->
        bytes = byte_size(key) + byte_size(value)
        {batch_bytes + bytes, record_bytes + @bitcask_record_header_size + bytes, delete_count}

      {:delete, key, _prob_path}, {batch_bytes, record_bytes, delete_count} ->
        bytes = byte_size(key)

        {batch_bytes + bytes, record_bytes + @bitcask_record_header_size + bytes,
         delete_count + 1}
    end)
  end

  defp bitcask_record_bytes(batch) do
    {_batch_bytes, record_bytes, _delete_count} = bitcask_batch_stats(batch)
    record_bytes
  end

  defp track_bitcask_append_bytes(state, file_path, file_id, written_bytes)
       when written_bytes > 0 do
    state = %{state | active_file_path: file_path, active_file_id: file_id}
    fid = state.active_file_id
    {total, dead} = Map.get(state.file_stats, fid, {0, 0})

    state
    |> Map.put(:active_file_size, state.active_file_size + written_bytes)
    |> Map.put(:file_stats, Map.put(state.file_stats, fid, {total + written_bytes, dead}))
    |> maybe_rotate_state_machine_active_file()
  end

  defp track_bitcask_append_bytes(state, _file_path, _file_id, _written_bytes), do: state

  defp track_cross_shard_append_bytes(state, shard_index, file_path, file_id, written_bytes) do
    if shard_index == state.shard_index do
      track_bitcask_append_bytes(state, file_path, file_id, written_bytes)
    else
      maybe_rotate_remote_cross_shard_active_file(
        state,
        shard_index,
        file_path,
        file_id,
        written_bytes
      )

      state
    end
  end

  defp maybe_rotate_remote_cross_shard_active_file(
         state,
         shard_index,
         file_path,
         file_id,
         written_bytes
       )
       when written_bytes > 0 do
    ctx = checkpoint_ctx_for_state(state)

    with %{keydir_refs: keydir_refs} <- ctx,
         true <- is_tuple(keydir_refs),
         true <- shard_index >= 0 and shard_index < tuple_size(keydir_refs),
         keydir <- elem(keydir_refs, shard_index),
         {^file_id, ^file_path, shard_data_path} <-
           Ferricstore.Store.ActiveFile.get(ctx, shard_index),
         {:ok, %{size: active_file_size}} <- File.stat(file_path) do
      max_active_file_size =
        Map.get(ctx, :max_active_file_size, Map.get(state, :max_active_file_size))

      %{
        state
        | shard_index: shard_index,
          shard_data_path: shard_data_path,
          shard_data_path_expanded: Path.expand(shard_data_path),
          active_file_id: file_id,
          active_file_path: file_path,
          active_file_size: active_file_size,
          file_stats: %{file_id => {active_file_size, 0}},
          max_active_file_size: max_active_file_size,
          ets: keydir
      }
      |> maybe_rotate_state_machine_active_file()
    end

    :ok
  rescue
    _ -> :ok
  end

  defp maybe_rotate_remote_cross_shard_active_file(
         _state,
         _shard_index,
         _file_path,
         _file_id,
         _written_bytes
       ),
       do: :ok

  defp mark_cross_shard_checkpoint_dirty(state, shard_index) do
    case checkpoint_ctx_for_state(state) do
      nil ->
        if shard_index == state.shard_index do
          clear_disk_pressure(state)
        end

      ctx ->
        flag_idx = shard_index + 1

        if flag_idx <= :atomics.info(ctx.checkpoint_flags).size do
          if shard_index == state.shard_index do
            remember_checkpoint_clean_before_write(state, ctx)
          end

          remember_checkpoint_dependencies_clean_before_write(state)
          :atomics.put(ctx.checkpoint_flags, flag_idx, 1)
          record_checkpoint_dirty_index(shard_index)
        end

        Ferricstore.Store.DiskPressure.clear(ctx, shard_index)
    end

    state
  rescue
    _ -> state
  end

  defp maybe_rotate_state_machine_active_file(state) do
    rotated =
      state
      |> Map.put(:index, state.shard_index)
      |> Map.put(:keydir, state.ets)
      |> ShardFlush.maybe_rotate_file()

    %{
      state
      | active_file_id: rotated.active_file_id,
        active_file_path: rotated.active_file_path,
        active_file_size: rotated.active_file_size,
        file_stats: rotated.file_stats
    }
  end

  defp append_pending_batch(file_path, batch) do
    has_delete? = pending_batch_has_delete?(batch)

    if standalone_staged_apply?() do
      append_pending_batch_sync(file_path, batch, has_delete?)
    else
      append_pending_batch_nosync(file_path, batch, has_delete?)
    end
  end

  defp append_pending_batch(file_path, batch, has_delete?) do
    if standalone_staged_apply?() do
      append_pending_batch_sync(file_path, batch, has_delete?)
    else
      append_pending_batch_nosync(file_path, batch, has_delete?)
    end
  end

  defp append_pending_batch_sync(file_path, batch, has_delete?) do
    case standalone_durability_hook(file_path, batch) do
      :passthrough ->
        do_append_pending_batch_sync(file_path, batch, has_delete?)

      {:error, _reason} = error ->
        error

      {:ok, _locations} = ok ->
        ok

      other ->
        other
    end
  end

  defp do_append_pending_batch_sync(file_path, batch, has_delete?) do
    if has_delete? do
      ops =
        Enum.map(batch, fn
          {:put, key, value, expire_at_ms} -> {:put, key, value, expire_at_ms}
          {:put_cold, key, value, expire_at_ms, _lfu} -> {:put, key, value, expire_at_ms}
          {:delete, key, _prob_path} -> {:delete, key}
        end)

      case NIF.v2_append_ops_batch_nosync(file_path, ops) do
        {:ok, locations} ->
          case NIF.v2_fsync(file_path) do
            :ok -> {:ok, locations}
            {:error, reason} -> {:error, reason}
          end

        {:error, _reason} = error ->
          error
      end
    else
      puts =
        Enum.map(batch, fn
          {:put, key, value, expire_at_ms} -> {key, value, expire_at_ms}
          {:put_cold, key, value, expire_at_ms, _lfu} -> {key, value, expire_at_ms}
        end)

      case NIF.v2_append_batch(file_path, puts) do
        {:ok, locations} ->
          tagged_locations =
            Enum.map(locations, fn {offset, value_size} ->
              {:put, offset, value_size}
            end)

          {:ok, tagged_locations}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp append_pending_batch_nosync(file_path, batch, has_delete?) do
    if has_delete? do
      ops =
        Enum.map(batch, fn
          {:put, key, value, expire_at_ms} -> {:put, key, value, expire_at_ms}
          {:put_cold, key, value, expire_at_ms, _lfu} -> {:put, key, value, expire_at_ms}
          {:delete, key, _prob_path} -> {:delete, key}
        end)

      NIF.v2_append_ops_batch_nosync(file_path, ops)
    else
      puts =
        Enum.map(batch, fn
          {:put, key, value, expire_at_ms} -> {key, value, expire_at_ms}
          {:put_cold, key, value, expire_at_ms, _lfu} -> {key, value, expire_at_ms}
        end)

      case NIF.v2_append_batch_nosync(file_path, puts) do
        {:ok, locations} ->
          tagged_locations =
            Enum.map(locations, fn {offset, value_size} ->
              {:put, offset, value_size}
            end)

          {:ok, tagged_locations}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp pending_batch_has_delete?(batch) do
    case Process.get(:sm_pending_has_delete, :unknown) do
      true -> true
      false -> false
      _ -> batch_contains_delete?(batch)
    end
  end

  defp batch_contains_delete?(batch), do: Enum.any?(batch, &match?({:delete, _, _}, &1))

  defp standalone_durability_hook(file_path, batch) do
    case Application.get_env(:ferricstore, :standalone_durability_hook) do
      hook when is_function(hook, 2) -> hook.(file_path, batch)
      _ -> :passthrough
    end
  end

  defp validate_append_result(batch, {:ok, locations}) do
    case validate_pending_locations(batch, locations) do
      :ok -> {:ok, locations}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_append_result(_batch, append_result), do: append_result

  defp validate_pending_locations(batch, locations) do
    validate_pending_locations(batch, locations, 0)
  end

  defp validate_pending_locations([], [], _index), do: :ok

  defp validate_pending_locations([], locations, index) do
    {:error,
     {:bitcask_append_result_mismatch, {:length_mismatch, index, index + length(locations)}}}
  end

  defp validate_pending_locations(entries, [], index) do
    {:error,
     {:bitcask_append_result_mismatch, {:length_mismatch, index + length(entries), index}}}
  end

  defp validate_pending_locations([entry | entries], [location | locations], index) do
    expected = pending_entry_op(entry)
    actual = pending_location_op(location)

    cond do
      expected != actual ->
        {:error, {:bitcask_append_result_mismatch, {:op_mismatch, index, expected, actual}}}

      not valid_pending_location?(location) ->
        {:error, {:bitcask_append_result_mismatch, {:invalid_location, index, location}}}

      true ->
        validate_pending_locations(entries, locations, index + 1)
    end
  end

  defp pending_entry_op({:put, _key, _value, _expire_at_ms}), do: :put
  defp pending_entry_op({:put_cold, _key, _value, _expire_at_ms, _lfu}), do: :put
  defp pending_entry_op({:delete, _key, _prob_path}), do: :delete

  defp pending_location_op({:put, _offset, _value_size}), do: :put
  defp pending_location_op({:delete, _offset, _record_size}), do: :delete
  defp pending_location_op(_location), do: :unknown

  defp valid_pending_location?({:put, offset, value_size}),
    do: non_negative_integer?(offset) and non_negative_integer?(value_size)

  defp valid_pending_location?({:delete, offset, record_size}),
    do: non_negative_integer?(offset) and non_negative_integer?(record_size)

  defp valid_pending_location?(_location), do: false

  defp non_negative_integer?(value), do: is_integer(value) and value >= 0

  defp apply_pending_locations(state, file_id, batch, locations) do
    cond do
      Process.get(:sm_pending_fast_put_batch) == true and put_only_pending_batch?(batch) ->
        apply_fast_put_pending_locations(
          state,
          file_id,
          batch,
          locations,
          hot_cache_threshold(state)
        )

      Process.get(:sm_pending_fast_delete_batch) == true and delete_only_pending_batch?(batch) ->
        apply_fast_delete_pending_locations(state, batch, locations)

      Process.get(:sm_pending_fast_staged_put_batch) == true and
          put_or_put_cold_pending_batch?(batch) ->
        apply_fast_staged_put_pending_locations(
          state,
          file_id,
          batch,
          locations,
          hot_cache_threshold(state)
        )

      true ->
        apply_pending_locations(state, file_id, batch, locations, standalone_staged_apply?())
    end
  end

  defp put_only_pending_batch?(batch) do
    Enum.all?(batch, fn
      {:put, _key, _value, _expire_at_ms} -> true
      _entry -> false
    end)
  end

  defp delete_only_pending_batch?(batch) do
    Enum.all?(batch, fn
      {:delete, _key, _prob_path} -> true
      _entry -> false
    end)
  end

  defp put_or_put_cold_pending_batch?(batch) do
    Enum.all?(batch, fn
      {:put, _key, _value, _expire_at_ms} -> true
      {:put_cold, _key, _value, _expire_at_ms, _lfu} -> true
      _entry -> false
    end)
  end

  defp apply_fast_put_pending_locations(_state, _file_id, [], [], _hot_threshold), do: :ok

  defp apply_fast_put_pending_locations(
         state,
         file_id,
         [{:put, key, value, expire_at_ms} | batch],
         [{:put, offset, value_size} | locations],
         hot_threshold
       ) do
    ets_val = value_for_ets(value, hot_threshold)
    previous = :ets.lookup(state.ets, key)

    track_keydir_binary_delta_from_previous(state, key, previous, ets_val, expire_at_ms)

    :ets.insert(
      state.ets,
      {key, ets_val, expire_at_ms, LFU.initial(), file_id, offset, value_size}
    )

    apply_fast_put_pending_locations(state, file_id, batch, locations, hot_threshold)
  end

  defp apply_fast_delete_pending_locations(_state, [], []), do: :ok

  defp apply_fast_delete_pending_locations(
         state,
         [{:delete, key, prob_path} | batch],
         [{:delete, _offset, _record_size} | locations]
       ) do
    track_keydir_binary_remove(state, key)
    :ets.delete(state.ets, key)
    maybe_queue_lmdb_state_delete_after_publish(state, key)
    maybe_delete_prob_file_path(state, prob_path)

    apply_fast_delete_pending_locations(state, batch, locations)
  end

  defp apply_fast_staged_put_pending_locations(state, file_id, batch, locations, hot_threshold) do
    cond do
      batch_has_duplicate_put_key?(batch) ->
        apply_final_staged_put_pending_locations(state, file_id, batch, locations, hot_threshold)

      true ->
        do_apply_fast_staged_put_pending_locations(
          state,
          file_id,
          batch,
          locations,
          hot_threshold
        )
    end
  end

  defp do_apply_fast_staged_put_pending_locations(
         _state,
         _file_id,
         [],
         [],
         _hot_threshold
       ),
       do: :ok

  defp do_apply_fast_staged_put_pending_locations(
         state,
         file_id,
         [{:put, key, value, expire_at_ms} | batch],
         [{:put, offset, value_size} | locations],
         hot_threshold
       ) do
    expected_value = value_for_ets(value, hot_threshold)
    expected_staged_size = byte_size(to_disk_binary(value))

    case :ets.lookup(state.ets, key) do
      [{^key, ^expected_value, ^expire_at_ms, lfu, :pending, 0, ^expected_staged_size}] ->
        :ets.insert(
          state.ets,
          {key, expected_value, expire_at_ms, lfu, file_id, offset, value_size}
        )

      _other ->
        apply_put_pending_location(state, key, value, expire_at_ms, file_id, offset, value_size)
    end

    do_apply_fast_staged_put_pending_locations(state, file_id, batch, locations, hot_threshold)
  end

  defp do_apply_fast_staged_put_pending_locations(
         state,
         file_id,
         [{:put_cold, key, value, expire_at_ms, lfu} | batch],
         [{:put, offset, value_size} | locations],
         hot_threshold
       ) do
    expected_staged_size = byte_size(to_disk_binary(value))

    case :ets.lookup(state.ets, key) do
      [{^key, nil, ^expire_at_ms, ^lfu, :pending, 0, ^expected_staged_size}] ->
        :ets.insert(state.ets, {key, nil, expire_at_ms, lfu, file_id, offset, value_size})

      _other ->
        apply_put_cold_pending_location(
          state,
          key,
          value,
          expire_at_ms,
          lfu,
          file_id,
          offset,
          value_size
        )
    end

    do_apply_fast_staged_put_pending_locations(state, file_id, batch, locations, hot_threshold)
  end

  defp batch_has_duplicate_put_key?([_, _ | _] = batch) do
    batch
    |> Enum.reduce_while(MapSet.new(), fn
      {:put, key, _value, _expire_at_ms}, seen ->
        if MapSet.member?(seen, key), do: {:halt, true}, else: {:cont, MapSet.put(seen, key)}

      {:put_cold, key, _value, _expire_at_ms, _lfu}, seen ->
        if MapSet.member?(seen, key), do: {:halt, true}, else: {:cont, MapSet.put(seen, key)}

      _entry, seen ->
        {:cont, seen}
    end)
    |> case do
      true -> true
      _seen -> false
    end
  end

  defp batch_has_duplicate_put_key?(_batch), do: false

  defp apply_final_staged_put_pending_locations(state, file_id, batch, locations, _hot_threshold) do
    final_indexes = final_put_key_indexes(batch)

    batch
    |> Enum.zip(locations)
    |> Enum.with_index()
    |> Enum.each(fn
      {{{:put, key, value, expire_at_ms}, {:put, offset, value_size}}, index} ->
        if Map.get(final_indexes, key) == index do
          apply_put_pending_location(state, key, value, expire_at_ms, file_id, offset, value_size)
        end

      {{{:put_cold, key, value, expire_at_ms, lfu}, {:put, offset, value_size}}, index} ->
        if Map.get(final_indexes, key) == index do
          apply_put_cold_pending_location(
            state,
            key,
            value,
            expire_at_ms,
            lfu,
            file_id,
            offset,
            value_size
          )
        end

      _other ->
        :ok
    end)

    :ok
  end

  defp final_put_key_indexes(batch) do
    batch
    |> Enum.with_index()
    |> Enum.reduce(%{}, fn
      {{:put, key, _value, _expire_at_ms}, index}, acc -> Map.put(acc, key, index)
      {{:put_cold, key, _value, _expire_at_ms, _lfu}, index}, acc -> Map.put(acc, key, index)
      {_entry, _index}, acc -> acc
    end)
  end

  defp apply_pending_locations(_state, _file_id, [], [], _staged?), do: :ok

  defp apply_pending_locations(
         state,
         file_id,
         [{:put, key, val, exp} | batch],
         [{:put, offset, value_size} | locations],
         staged?
       ) do
    apply_put_pending_location(state, key, val, exp, file_id, offset, value_size)
    apply_pending_locations(state, file_id, batch, locations, staged?)
  end

  defp apply_pending_locations(
         state,
         file_id,
         [{:put_cold, key, val, exp, lfu} | batch],
         [{:put, offset, value_size} | locations],
         staged?
       ) do
    apply_put_cold_pending_location(state, key, val, exp, lfu, file_id, offset, value_size)
    apply_pending_locations(state, file_id, batch, locations, staged?)
  end

  defp apply_pending_locations(
         state,
         file_id,
         [{:delete, key, nil} | batch],
         [{:delete, _offset, _record_size} | locations],
         staged?
       ) do
    if staged? do
      track_keydir_binary_remove(state, key)
      :ets.delete(state.ets, key)
      maybe_queue_lmdb_state_delete_after_publish(state, key)
    end

    apply_pending_locations(state, file_id, batch, locations, staged?)
  end

  defp apply_pending_locations(
         state,
         file_id,
         [{:delete, key, prob_path} | batch],
         [{:delete, _offset, _record_size} | locations],
         staged?
       ) do
    if staged? do
      track_keydir_binary_remove(state, key)
      :ets.delete(state.ets, key)
      maybe_queue_lmdb_state_delete_after_publish(state, key)
    end

    maybe_delete_prob_file_path(state, prob_path)
    apply_pending_locations(state, file_id, batch, locations, staged?)
  end

  defp apply_put_cold_pending_location(
         state,
         key,
         value,
         expire_at_ms,
         lfu,
         file_id,
         offset,
         value_size
       ) do
    if standalone_staged_apply?() do
      track_keydir_binary_delta(state, key, nil, expire_at_ms)
      :ets.insert(state.ets, {key, nil, expire_at_ms, lfu, file_id, offset, value_size})
    else
      expected_staged_size = byte_size(to_disk_binary(value))

      replace_pending_location(
        state,
        key,
        nil,
        expire_at_ms,
        expected_staged_size,
        file_id,
        offset,
        value_size
      )
    end

    :ok
  end

  defp apply_put_pending_location(state, key, value, expire_at_ms, file_id, offset, value_size) do
    expected_value = value_for_ets(value, hot_cache_threshold(state))
    expected_staged_size = byte_size(to_disk_binary(value))

    if standalone_staged_apply?() do
      track_keydir_binary_delta(state, key, expected_value, expire_at_ms)

      :ets.insert(
        state.ets,
        {key, expected_value, expire_at_ms, LFU.initial(), file_id, offset, value_size}
      )
    else
      replaced =
        replace_pending_location(
          state,
          key,
          expected_value,
          expire_at_ms,
          expected_staged_size,
          file_id,
          offset,
          value_size
        )

      if replaced == 0 and expected_staged_size != 0 do
        # Router-originated async writes stage small values with vsize=0; Ra apply
        # must still CAS on value/expiry so stale append results cannot publish.
        replace_pending_location(
          state,
          key,
          expected_value,
          expire_at_ms,
          0,
          file_id,
          offset,
          value_size
        )
      end
    end

    :ok
  end

  defp replace_pending_location(
         state,
         key,
         expected_value,
         expire_at_ms,
         expected_staged_size,
         file_id,
         offset,
         value_size
       ) do
    try do
      :ets.select_replace(state.ets, [
        {
          {key, expected_value, expire_at_ms, :"$1", :pending, 0, expected_staged_size},
          [],
          [{{key, expected_value, expire_at_ms, :"$1", file_id, offset, value_size}}]
        }
      ])
    rescue
      ArgumentError -> 0
    end
  end

  defp queue_pending_put(key, value, expire_at_ms) do
    pending = Process.get(:sm_pending_writes, [])
    Process.put(:sm_pending_writes, [{:put, key, value, expire_at_ms} | pending])
    pending_values = Process.get(:sm_pending_values, %{})
    Process.put(:sm_pending_values, Map.put(pending_values, key, {value, expire_at_ms}))
  end

  defp queue_pending_put_cold(key, value, expire_at_ms, lfu) do
    pending = Process.get(:sm_pending_writes, [])
    Process.put(:sm_pending_writes, [{:put_cold, key, value, expire_at_ms, lfu} | pending])
    pending_values = Process.get(:sm_pending_values, %{})
    Process.put(:sm_pending_values, Map.put(pending_values, key, {value, expire_at_ms}))
  end

  defp queue_pending_flow_history_projection(entry) do
    pending = Process.get(:sm_pending_flow_history_projections, [])
    Process.put(:sm_pending_flow_history_projections, [entry | pending])
    :ok
  end

  defp queue_pending_flow_history_projections_batch([]), do: :ok

  defp queue_pending_flow_history_projections_batch(entries) when is_list(entries) do
    pending = Process.get(:sm_pending_flow_history_projections, [])
    Process.put(:sm_pending_flow_history_projections, Enum.reverse(entries, pending))
    :ok
  end

  defp publish_pending_flow_history_projections(state) do
    case Process.get(:sm_pending_flow_history_projections, []) do
      [] ->
        :ok

      pending when is_list(pending) ->
        entries = Enum.reverse(pending)
        ra_index = current_ra_index()
        ctx = checkpoint_ctx_for_state(state)

        entries
        |> Enum.group_by(&flow_history_projection_shard(ctx, state, &1))
        |> Enum.reduce_while(:ok, fn {shard_index, shard_entries}, :ok ->
          case publish_pending_flow_history_projection_shard(
                 state,
                 ctx,
                 shard_index,
                 shard_entries,
                 ra_index
               ) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
    end
  end

  defp flow_history_projection_shard(ctx, state, %{key: key})
       when is_map(ctx) and is_binary(key) do
    Router.shard_for(ctx, key)
  rescue
    _ -> state.shard_index
  catch
    :exit, _ -> state.shard_index
  end

  defp flow_history_projection_shard(nil, state, %{key: key})
       when is_binary(key) do
    state.shard_index
  end

  defp flow_history_projection_shard(_ctx, state, _entry), do: state.shard_index

  defp publish_pending_flow_history_projection_shard(
         state,
         ctx,
         shard_index,
         entries,
         ra_index
       ) do
    result =
      if sync_flow_history_projection?() do
        # WARaft storage metadata is the replay boundary. A staged standalone
        # apply cannot advance that position after merely enqueueing async
        # history projection, otherwise crash recovery can skip committed
        # commands whose Flow history was still only in projector memory.
        HistoryProjector.write_entries_sync(
          ctx,
          shard_index,
          flow_history_projection_shard_data_path(state, ctx, shard_index),
          entries,
          ra_index,
          flow_history_projection_opts(state, ctx)
        )
      else
        case HistoryProjector.enqueue(ctx, shard_index, entries, ra_index) do
          :ok ->
            :ok

          {:error, _reason} ->
            HistoryProjector.write_entries_sync(
              ctx,
              shard_index,
              flow_history_projection_shard_data_path(state, ctx, shard_index),
              entries,
              ra_index,
              flow_history_projection_opts(state, ctx)
            )
        end
      end

    case result do
      :ok -> :ok
      {:error, reason} -> {:error, {:flow_history_projection_failed, reason}}
    end
  end

  defp flow_history_projection_shard_data_path(_state, %{data_dir: data_dir}, shard_index)
       when is_binary(data_dir) do
    Ferricstore.DataDir.shard_data_path(data_dir, shard_index)
  end

  defp flow_history_projection_shard_data_path(state, _ctx, _shard_index),
    do: state.shard_data_path

  defp flow_history_projection_opts(%{ets: keydir}, nil), do: [keydir: keydir]
  defp flow_history_projection_opts(_state, _ctx), do: []

  defp queue_pending_delete(key, prob_path) do
    pending = Process.get(:sm_pending_writes, [])
    Process.put(:sm_pending_writes, [{:delete, key, prob_path} | pending])
    Process.put(:sm_pending_has_delete, true)
    pending_values = Process.get(:sm_pending_values, %{})
    Process.put(:sm_pending_values, Map.put(pending_values, key, :deleted))
  end

  defp queue_pending_delete_fast(key, prob_path) do
    pending = Process.get(:sm_pending_writes, [])
    Process.put(:sm_pending_writes, [{:delete, key, prob_path} | pending])
    Process.put(:sm_pending_has_delete, true)
  end

  defp standalone_staged_apply?, do: Process.get(@sm_standalone_staged_key) == true

  defp sync_flow_history_projection?,
    do:
      standalone_staged_apply?() or
        Process.get(@sm_force_sync_flow_history_key) == true

  defp emit_raft_apply_telemetry(state, started_at, result, flush_result) do
    :telemetry.execute(
      [:ferricstore, :raft, :apply],
      %{duration_us: duration_us(started_at)},
      %{
        shard_index: state.shard_index,
        result: result_class(result),
        disk: flush_result_class(flush_result)
      }
    )
  end

  defp emit_bitcask_append_telemetry(
         state,
         started_at,
         batch_size,
         batch_bytes,
         delete_count,
         append_result
       ) do
    :telemetry.execute(
      [:ferricstore, :bitcask, :append],
      %{
        duration_us: duration_us(started_at),
        batch_size: batch_size,
        batch_bytes: batch_bytes,
        delete_count: delete_count
      },
      %{shard_index: state.shard_index, status: append_result_class(append_result)}
    )
  end

  defp result_class({:error, _}), do: :error
  defp result_class(_), do: :ok

  defp flush_result_class(:ok), do: :ok
  defp flush_result_class({:error, _}), do: :error
  defp flush_result_class(_), do: :unknown

  defp append_result_class({:ok, _}), do: :ok
  defp append_result_class({:error, _}), do: :error
  defp append_result_class(:stale), do: :stale
  defp append_result_class(_), do: :unknown

  defp set_disk_pressure(state) do
    case checkpoint_ctx_for_state(state) do
      nil ->
        Ferricstore.Store.DiskPressure.set(state.shard_index)

      ctx ->
        Ferricstore.Store.DiskPressure.set(ctx, state.shard_index)
    end
  end

  defp clear_disk_pressure(state) do
    case checkpoint_ctx_for_state(state) do
      nil ->
        Ferricstore.Store.DiskPressure.clear(state.shard_index)

      ctx ->
        flag_idx = state.shard_index + 1

        if flag_idx <= :atomics.info(ctx.checkpoint_flags).size do
          remember_checkpoint_clean_before_write(state, ctx)
          remember_checkpoint_dependencies_clean_before_write(state)
          :atomics.put(ctx.checkpoint_flags, flag_idx, 1)
          record_checkpoint_dirty_index(state.shard_index)
        end

        Ferricstore.Store.DiskPressure.clear(ctx, state.shard_index)
    end
  end

  defp remember_checkpoint_clean_before_write(state, ctx) do
    if apply_state_get(:checkpoint_clean_before_write) != true and
         checkpoint_clean?(%{state | instance_ctx: ctx}) do
      apply_state_put(:checkpoint_clean_before_write, true)
    end
  rescue
    _ -> :ok
  end

  defp checkpoint_ctx_for_state(%{instance_ctx: ctx}) when is_map(ctx), do: ctx

  defp checkpoint_ctx_for_state(%{instance_name: :default} = state) do
    case instance_ctx_by_name(:default) do
      %FerricStore.Instance{} = ctx ->
        if instance_data_path?(ctx, state), do: ctx, else: nil

      _ ->
        nil
    end
  end

  defp checkpoint_ctx_for_state(%{instance_name: name}) when is_atom(name) do
    instance_ctx_by_name(name)
  end

  defp checkpoint_ctx_for_state(_state), do: nil

  defp instance_data_path?(
         %FerricStore.Instance{data_dir_expanded: data_dir},
         %{shard_data_path_expanded: shard_data_path}
       )
       when is_binary(data_dir) and is_binary(shard_data_path) do
    shard_data_path == data_dir or String.starts_with?(shard_data_path, data_dir <> "/")
  end

  defp instance_data_path?(_ctx, _state), do: false

  defp initial_file_stats(shard_data_path, ets, active_file_id) do
    stats = ShardFlush.compute_file_stats(shard_data_path, ets)

    Map.put_new(
      stats,
      active_file_id,
      {file_size_or_zero(bitcask_file_path(shard_data_path, active_file_id)), 0}
    )
  end

  defp bitcask_file_path(shard_data_path, file_id) do
    Path.join(shard_data_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  defp file_size_or_zero(path) do
    case File.stat(path) do
      {:ok, %{size: size}} -> size
      _ -> 0
    end
  end

  defp default_merge_config do
    %{
      fragmentation_threshold: @default_fragmentation_threshold,
      dead_bytes_threshold: @default_dead_bytes_threshold
    }
  end

  defp flow_async_history_config(config) do
    Map.get_lazy(config, :flow_async_history, fn ->
      case Application.get_env(:ferricstore, :flow_async_history) do
        value when value in [true, "1", "true"] ->
          true

        _ ->
          System.get_env("FLOW_ASYNC_HISTORY") in ["1", "true"]
      end
    end)
  end

  defp duration_us(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :microsecond)
  end

  defp record_pending_original(state, key) do
    originals = Process.get(:sm_pending_originals, %{})
    previous = :ets.lookup(state.ets, key)
    updated = record_pending_original_from_previous(key, previous, originals)

    if updated != originals do
      Process.put(:sm_pending_originals, updated)
    end
  end

  defp record_pending_original_from_previous(key, previous, originals) do
    if Map.has_key?(originals, key) do
      originals
    else
      Map.put(originals, key, pending_original_from_previous(previous))
    end
  end

  defp pending_original_from_previous([entry]), do: {:entry, entry}
  defp pending_original_from_previous([]), do: :missing

  defp flow_lmdb_mirror?(%{flow_lmdb_mirror?: mirror?}) when is_boolean(mirror?), do: mirror?
  defp flow_lmdb_mirror?(_state), do: Ferricstore.Flow.LMDB.mirror?()

  defp flow_lmdb_record_path(state), do: Map.fetch!(state, :flow_lmdb_path)

  defp queue_pending_lmdb_mirror_put(key, value, expire_at_ms) do
    blob = Ferricstore.Flow.LMDB.encode_value(value, expire_at_ms)

    queue_pending_lmdb_mirror_op({:put, key, blob})

    pending_values = Process.get(:sm_pending_lmdb_values, %{})
    Process.put(:sm_pending_lmdb_values, Map.put(pending_values, key, {:put, blob}))
    :ok
  end

  defp maybe_queue_lmdb_indexes_for_state_record(state, state_key, value, expire_at_ms, record)
       when is_map(record) do
    with_lmdb_mirror_shard(state, fn ->
      if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
        queue_pending_lmdb_mirror_put(state_key, value, expire_at_ms)
        queue_terminal_lmdb_index_put(state, state_key, record, expire_at_ms)
      end
    end)

    :ok
  end

  defp queue_terminal_lmdb_index_put(state, state_key, record, expire_at_ms) do
    partition_key = Map.get(record, :partition_key)
    updated_at_ms = Map.get(record, :updated_at_ms, 0)

    queue_pending_lmdb_mirror_terminal_project(
      Map.fetch!(record, :id),
      Map.fetch!(record, :type),
      Map.fetch!(record, :state),
      partition_key,
      updated_at_ms,
      state_key,
      expire_at_ms,
      Map.get(record, :parent_flow_id),
      Map.get(record, :root_flow_id),
      Map.get(record, :correlation_id)
    )

    queue_pending_lmdb_mirror_after_flush(
      {:defer_after_flush, flow_terminal_hot_ttl_ms(),
       {:prune_terminal_flow_v2, state.ets, Map.get(state, :zset_score_index_name),
        Map.get(state, :zset_score_lookup_name), Map.get(state, :flow_index_name),
        Map.get(state, :flow_lookup_name), state_key, Map.fetch!(record, :type),
        Map.fetch!(record, :state), partition_key, Map.get(record, :parent_flow_id),
        Map.get(record, :root_flow_id), Map.get(record, :correlation_id), Map.fetch!(record, :id),
        Map.fetch!(record, :version)}}
    )
  end

  defp queue_pending_lmdb_mirror_terminal_project(
         id,
         type,
         terminal_state,
         partition_key,
         updated_at_ms,
         state_key,
         expire_at_ms,
         parent_flow_id,
         root_flow_id,
         correlation_id
       ) do
    queue_pending_lmdb_mirror_op(
      {:terminal_project, id, type, terminal_state, partition_key, updated_at_ms, state_key,
       expire_at_ms, parent_flow_id, root_flow_id, correlation_id}
    )

    :ok
  end

  defp flow_terminal_hot_ttl_ms do
    :ferricstore
    |> Application.get_env(:flow_terminal_hot_ttl_ms, 3_600_000)
    |> normalize_non_negative_integer(3_600_000)
  end

  defp normalize_non_negative_integer(value, _default)
       when is_integer(value) and value >= 0,
       do: value

  defp normalize_non_negative_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {parsed, ""} when parsed >= 0 -> parsed
      _other -> default
    end
  end

  defp normalize_non_negative_integer(_value, default), do: default

  defp maybe_queue_terminal_lmdb_index_delete(state, record) do
    with_lmdb_mirror_shard(state, fn ->
      if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
        partition_key = Map.get(record, :partition_key)
        state_index_key = FlowKeys.state_index_key(record.type, record.state, partition_key)
        updated_at_ms = Map.get(record, :updated_at_ms, 0)

        state_index_key
        |> Ferricstore.Flow.LMDB.terminal_index_key(record.id, updated_at_ms)
        |> queue_pending_lmdb_mirror_terminal_delete(
          FlowKeys.state_key(record.id, Map.get(record, :partition_key)),
          Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)
        )
      end
    end)

    :ok
  end

  defp queue_lmdb_metadata_index_deletes(state, record) do
    with_lmdb_mirror_shard(state, fn ->
      record
      |> flow_metadata_index_entries()
      |> Enum.each(fn {index_key, id, score} ->
        index_key
        |> Ferricstore.Flow.LMDB.query_index_key(id, score)
        |> queue_pending_lmdb_mirror_query_delete()
      end)
    end)

    :ok
  end

  defp queue_lmdb_history_indexes_project_from_index(state, record, history_key) do
    queue_pending_lmdb_mirror_op(
      {:history_project_from_index, Map.get(state, :flow_index_name),
       Map.get(state, :flow_lookup_name), Map.fetch!(record, :id),
       Map.get(record, :partition_key), history_key, flow_record_expire_at(record)}
    )

    :ok
  end

  defp queue_lmdb_history_index_delete(_record, history_key, event_id, event_ms) do
    history_key
    |> Ferricstore.Flow.LMDB.history_index_key(event_id, event_ms)
    |> queue_pending_lmdb_mirror_history_delete()
  end

  defp queue_pending_lmdb_mirror_history_delete(history_index_key) do
    queue_pending_lmdb_mirror_op({:history_delete, history_index_key})
    :ok
  end

  defp queue_pending_lmdb_mirror_query_delete(query_key) do
    queue_pending_lmdb_mirror_op({:query_delete, query_key})
    :ok
  end

  defp queue_pending_lmdb_mirror_delete(key) do
    queue_pending_lmdb_mirror_op({:delete, key})
    :ok
  end

  defp queue_pending_lmdb_mirror_terminal_delete(terminal_key, state_key, count_key) do
    op = {:terminal_delete, terminal_key, state_key, count_key}
    queue_pending_lmdb_mirror_op(op)
    :ok
  end

  defp queue_pending_lmdb_mirror_after_flush(action) do
    pending = Process.get(:sm_pending_lmdb_mirror_after_flush, [])

    item =
      case Process.get(:sm_pending_lmdb_mirror_shard) do
        shard_index when is_integer(shard_index) and shard_index >= 0 ->
          {:lmdb_shard, shard_index, action}

        _other ->
          action
      end

    Process.put(:sm_pending_lmdb_mirror_after_flush, [item | pending])
    :ok
  end

  defp queue_pending_lmdb_mirror_op(op) do
    pending = Process.get(:sm_pending_lmdb_mirror_ops, [])

    item =
      case Process.get(:sm_pending_lmdb_mirror_shard) do
        shard_index when is_integer(shard_index) and shard_index >= 0 ->
          {:lmdb_shard, shard_index, op}

        _other ->
          op
      end

    Process.put(:sm_pending_lmdb_mirror_ops, [item | pending])
  end

  defp with_lmdb_mirror_shard(state, fun) when is_function(fun, 0) do
    shard_index = Map.get(state, :shard_index, 0)

    case Process.get(:sm_pending_lmdb_mirror_default_shard, shard_index) do
      ^shard_index ->
        fun.()

      _other ->
        with_tagged_lmdb_mirror_shard(shard_index, fun)
    end
  end

  defp with_tagged_lmdb_mirror_shard(shard_index, fun) do
    previous = Process.get(:sm_pending_lmdb_mirror_shard, :undefined)
    Process.put(:sm_pending_lmdb_mirror_shard, shard_index)
    Process.put(:sm_pending_lmdb_mirror_tagged, true)

    try do
      fun.()
    after
      case previous do
        :undefined -> Process.delete(:sm_pending_lmdb_mirror_shard)
        value -> Process.put(:sm_pending_lmdb_mirror_shard, value)
      end
    end
  end

  defp enqueue_pending_lmdb_mirror(state) do
    after_flush =
      case Process.put(:sm_pending_lmdb_mirror_after_flush, []) do
        pending when is_list(pending) -> Enum.reverse(pending)
        _ -> []
      end

    case Process.put(:sm_pending_lmdb_mirror_ops, []) do
      [] ->
        :ok

      pending when is_list(pending) ->
        enqueue_lmdb_mirror_groups(state, Enum.reverse(pending), after_flush)

      _ ->
        :ok
    end
  end

  defp enqueue_lmdb_mirror_groups(state, ops, after_flush) do
    if Process.get(:sm_pending_lmdb_mirror_tagged, false) do
      enqueue_tagged_lmdb_mirror_groups(state, ops, after_flush)
    else
      enqueue_lmdb_mirror_group(state, state.shard_index, ops, after_flush)
    end
  end

  defp enqueue_tagged_lmdb_mirror_groups(state, ops, after_flush) do
    op_groups = group_lmdb_mirror_items(ops, state.shard_index)
    after_flush_groups = group_lmdb_mirror_items(after_flush, state.shard_index)

    (Map.keys(op_groups) ++ Map.keys(after_flush_groups))
    |> Enum.uniq()
    |> Enum.reduce_while(:ok, fn shard_index, :ok ->
      shard_ops = Map.get(op_groups, shard_index, [])
      shard_after_flush = Map.get(after_flush_groups, shard_index, [])

      case enqueue_lmdb_mirror_group(state, shard_index, shard_ops, shard_after_flush) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
        other -> {:halt, other}
      end
    end)
  end

  defp enqueue_lmdb_mirror_group(state, shard_index, shard_ops, shard_after_flush) do
    case Ferricstore.Flow.LMDBWriter.enqueue(
           state.instance_name,
           shard_index,
           shard_ops,
           shard_after_flush
         ) do
      :ok -> :ok
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  defp group_lmdb_mirror_items(items, default_shard) do
    items
    |> Enum.reduce(%{}, fn
      {:lmdb_shard, shard_index, item}, acc when is_integer(shard_index) and shard_index >= 0 ->
        Map.update(acc, shard_index, [item], &[item | &1])

      item, acc ->
        Map.update(acc, default_shard, [item], &[item | &1])
    end)
    |> Map.new(fn {shard_index, shard_items} -> {shard_index, Enum.reverse(shard_items)} end)
  end

  defp observe_pending_lmdb_mirror_enqueue(_state, :ok), do: :ok

  defp observe_pending_lmdb_mirror_enqueue(state, {:error, reason}) do
    mark_flow_lmdb_mirror_degraded(state, reason)
    :ok
  end

  defp observe_pending_lmdb_mirror_enqueue(_state, _other), do: :ok

  defp mark_flow_lmdb_mirror_degraded(state, reason) do
    ctx = Map.get(state, :instance_ctx)
    shard_index = Map.get(state, :shard_index, 0)
    flag_idx = shard_index + 1

    flow_lmdb_safe_atomic_update(
      Map.get(ctx || %{}, :flow_lmdb_mirror_enqueue_failures),
      flag_idx,
      fn ref, idx -> :atomics.add(ref, idx, 1) end
    )

    flow_lmdb_safe_atomic_update(
      Map.get(ctx || %{}, :flow_lmdb_mirror_degraded),
      flag_idx,
      fn ref, idx -> :atomics.put(ref, idx, 1) end
    )

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_mirror, :degraded],
      %{count: 1},
      %{
        instance_name: Map.get(state, :instance_name, :default),
        shard_index: shard_index,
        reason: reason
      }
    )
  end

  defp flow_lmdb_safe_atomic_update(ref, flag_idx, fun)
       when is_reference(ref) and is_integer(flag_idx) and flag_idx > 0 and is_function(fun, 2) do
    if flag_idx <= :atomics.info(ref).size do
      fun.(ref, flag_idx)
    else
      :ok
    end
  rescue
    _ -> :ok
  end

  defp flow_lmdb_safe_atomic_update(_ref, _flag_idx, _fun), do: :ok

  defp flush_pending_lmdb(_state), do: :ok

  defp rollback_pending_lmdb(_state), do: :ok

  defp rollback_pending_writes(state) do
    rollback_pending_lmdb(state)
    rollback_pending_flow_indexes()
    rollback_pending_prob_creates(state)

    Process.get(:sm_pending_originals, %{})
    |> Enum.each(fn
      {key, {:entry, entry}} ->
        track_keydir_binary_restore(state, key, entry)
        :ets.insert(state.ets, entry)

      {key, :missing} ->
        track_keydir_binary_restore(state, key, nil)
        :ets.delete(state.ets, key)
    end)
  end

  defp rollback_pending_prob_creates(state) do
    :sm_pending_prob_creates
    |> Process.get([])
    |> Enum.uniq()
    |> Enum.each(fn path ->
      cleanup_created_prob_file(state, path)
    end)
  end

  defp track_keydir_binary_restore(state, key, original_entry) do
    ref = keydir_binary_ref(state)

    if ref do
      current_bytes =
        case :ets.lookup(state.ets, key) do
          [{^key, value, _, _, _, _, _}] -> binary_byte_size(key) + binary_byte_size(value)
          _ -> 0
        end

      original_bytes =
        case original_entry do
          {^key, value, _, _, _, _, _} -> binary_byte_size(key) + binary_byte_size(value)
          _ -> 0
        end

      delta = original_bytes - current_bytes
      if delta != 0, do: :atomics.add(ref, state.shard_index + 1, delta)
    end
  end

  # Returns {path, file_id} for the active Bitcask log file. Prefer the live
  # ActiveFile registry so state-machine writes follow shard rotations even
  # when the init-time path still exists. Falls back to ra state for isolated
  # tests/recovery where the registry has not been published yet.
  defp resolve_active_file(state) do
    case live_active_file(state) do
      {file_path, file_id} ->
        {file_path, file_id}

      :stale ->
        if Ferricstore.FS.exists?(state.active_file_path) do
          {state.active_file_path, state.active_file_id}
        else
          :stale
        end
    end
  end

  defp live_active_file(state) do
    try do
      {file_id, file_path, _data_path} =
        Ferricstore.Store.ActiveFile.get(state.instance_ctx, state.shard_index)

      if Ferricstore.FS.exists?(file_path) do
        {file_path, file_id}
      else
        :stale
      end
    rescue
      _ -> :stale
    end
  end

  defp do_delete(state, key) do
    # If the key has a pending background write, flush the BitcaskWriter
    # first to ensure the PUT record lands on disk BEFORE the tombstone.
    # Without this, a background PUT arriving after the tombstone would
    # resurrect the key on recovery (Bitcask last-record-wins semantics).
    with :ok <- flush_pending_for_key(state, key) do
      prob_path = prob_file_path_for_delete(state, key)

      case resolve_active_file(state) do
        :stale ->
          set_disk_pressure(state)
          {:error, :active_file_unavailable}

        {_file_path, _file_id} ->
          record_pending_original(state, key)
          queue_pending_delete(key, prob_path)

          unless standalone_staged_apply?() do
            track_keydir_binary_remove(state, key)
            :ets.delete(state.ets, key)
            maybe_queue_lmdb_state_delete(state, key)
          end

          :ok
      end
    end
  end

  defp maybe_queue_lmdb_state_delete(state, key) when is_binary(key) do
    if flow_state_key?(key) do
      :ets.insert(state.ets, {key, nil, 0, :flow_state_deleted, :deleted, 0, 0})
      queue_lmdb_state_delete_projection(state, key)
    end

    :ok
  end

  defp maybe_queue_lmdb_state_delete(_state, _key), do: :ok

  defp maybe_queue_lmdb_state_delete_after_publish(state, key) when is_binary(key) do
    if flow_state_key?(key) do
      queue_lmdb_state_delete_projection(state, key)
    end

    :ok
  end

  defp maybe_queue_lmdb_state_delete_after_publish(_state, _key), do: :ok

  defp queue_lmdb_state_delete_projection(state, key) do
    with_lmdb_mirror_shard(state, fn ->
      queue_pending_lmdb_mirror_delete(key)
      queue_pending_lmdb_mirror_after_flush({:delete_flow_tombstone, state.ets, key})
    end)
  end

  defp maybe_queue_lmdb_policy_put(key, value, expire_at_ms) do
    if flow_lmdb_mirror?(nil) and FlowKeys.policy_key?(key) do
      queue_pending_lmdb_mirror_put(key, value, expire_at_ms)
    end

    :ok
  end

  defp flow_state_key?(key) when is_binary(key) do
    FlowKeys.state_key?(key)
  end

  # Flushes the BitcaskWriter if the key has a pending background write.
  # Called before tombstone writes and delete_prefix operations to ensure
  # correct disk ordering (PUT before TOMBSTONE).
  defp flush_pending_for_key(state, key) do
    case :ets.lookup(state.ets, key) do
      [{^key, _v, _e, _lfu, :pending, _off, _vs}] ->
        try do
          case BitcaskWriter.flush(state.instance_ctx, state.shard_index) do
            :ok ->
              :ok

            {:error, reason} ->
              Logger.warning(
                "Shard #{state.shard_index}: pending write flush failed before tombstone for #{inspect(key)}: #{inspect(reason)}"
              )

              {:error, {:bitcask_writer_flush_failed, reason}}
          end
        rescue
          error ->
            {:error, {:bitcask_writer_flush_failed, {:exception, error}}}
        catch
          :exit, reason ->
            {:error, {:bitcask_writer_flush_failed, {:exit, reason}}}
        end

      _ ->
        :ok
    end
  end

  # Returns nil for values exceeding the hot cache max value size threshold,
  # or the value itself if it fits. Prevents large values from being stored
  # in ETS, avoiding expensive binary copies on every :ets.lookup.
  @compile {:inline, value_for_ets: 2}
  defp value_for_ets(nil, _threshold), do: nil
  defp value_for_ets(value, _threshold) when is_integer(value), do: Integer.to_string(value)
  defp value_for_ets(value, _threshold) when is_float(value), do: Float.to_string(value)

  defp value_for_ets(value, threshold) when is_binary(value) do
    if byte_size(value) > threshold do
      nil
    else
      value
    end
  end

  # Catch-all for non-primitive values (e.g. tuples like {:topk_path, path}
  # stored via Ops.put). Serialize to binary for ETS storage.
  defp value_for_ets(value, _threshold), do: :erlang.term_to_binary(value)

  @compile {:inline, hot_cache_threshold: 1}
  defp hot_cache_threshold(%{instance_ctx: ctx}) when ctx != nil,
    do: Map.get(ctx, :hot_cache_max_value_size, 65_536)

  defp hot_cache_threshold(_state), do: 65_536

  defp to_disk_binary(v) when is_integer(v), do: Integer.to_string(v)
  defp to_disk_binary(v) when is_float(v), do: Float.to_string(v)
  defp to_disk_binary(v) when is_binary(v), do: v
  defp to_disk_binary(v), do: :erlang.term_to_binary(v)

  # ---------------------------------------------------------------------------
  # Private: string mutation operations
  # ---------------------------------------------------------------------------

  # Atomic INCR/DECR/INCRBY/DECRBY: reads current value, parses as integer,
  # adds delta, writes back. Preserves existing expire_at_ms.
  # Returns {:ok, new_integer} or {:error, reason}.
  # Enforces int64 bounds [-2^63, 2^63-1] to match Redis behavior.
  @int64_max 9_223_372_036_854_775_807
  @int64_min -9_223_372_036_854_775_808

  defp do_incr(state, key, delta) do
    with :ok <- ensure_string_key(state, key) do
      case do_get_meta(state, key) do
        nil ->
          if delta > @int64_max or delta < @int64_min do
            {:error, "ERR increment or decrement would overflow"}
          else
            do_put(state, key, delta, 0)
            {:ok, delta}
          end

        {value, expire_at_ms} ->
          case coerce_integer(value) do
            {:ok, int_val} ->
              new_val = int_val + delta

              if new_val > @int64_max or new_val < @int64_min do
                {:error, "ERR increment or decrement would overflow"}
              else
                do_put(state, key, new_val, expire_at_ms)
                {:ok, new_val}
              end

            :error ->
              {:error, "ERR value is not an integer or out of range"}
          end
      end
    end
  end

  # Parses a binary as an integer. Returns `{:ok, integer}` or `:error`.
  defp parse_integer(str) when is_binary(str) do
    case Integer.parse(str) do
      {val, ""} -> {:ok, val}
      _ -> :error
    end
  end

  # Coerces a value (integer, float, or binary) to integer.
  defp coerce_integer(v) when is_integer(v), do: {:ok, v}
  defp coerce_integer(v) when is_float(v), do: :error
  defp coerce_integer(v) when is_binary(v), do: parse_integer(v)

  # Coerces a value (integer, float, or binary) to float.
  defp coerce_float(v) when is_float(v), do: {:ok, v}
  defp coerce_float(v) when is_integer(v), do: {:ok, v * 1.0}
  defp coerce_float(v) when is_binary(v), do: parse_float(v)

  # Atomic INCRBYFLOAT: reads current value, parses as float, adds delta,
  # formats result, writes back. Preserves existing expire_at_ms.
  defp do_incr_float(state, key, delta) do
    with :ok <- ensure_string_key(state, key) do
      case do_get_meta(state, key) do
        nil ->
          new_val = delta * 1.0
          do_put(state, key, new_val, 0)
          {:ok, new_val}

        {value, expire_at_ms} ->
          case coerce_float(value) do
            {:ok, float_val} ->
              new_val = float_val + delta
              do_put(state, key, new_val, expire_at_ms)
              {:ok, new_val}

            :error ->
              {:error, "ERR value is not a valid float"}
          end
      end
    end
  end

  # Delegates to the shared ValueCodec to avoid duplication with shard.ex.
  defp parse_float(str), do: ValueCodec.parse_float(str)

  # Atomic APPEND: reads current value (or ""), concatenates suffix, writes
  # back. Preserves the existing expire_at_ms on the key.
  defp do_append(state, key, suffix) do
    with :ok <- ensure_string_key(state, key) do
      {old_val, expire_at_ms} =
        case do_get_meta(state, key) do
          nil -> {"", 0}
          {v, exp} -> {to_disk_binary(v), exp}
        end

      new_val = old_val <> suffix
      do_put(state, key, new_val, expire_at_ms)
      {:ok, byte_size(new_val)}
    end
  end

  # Atomic GETSET: reads old value, writes new value with no expiry, returns
  # old value directly (not wrapped in {:ok, ...}).
  defp do_getset(state, key, new_value) do
    with :ok <- ensure_string_key(state, key) do
      old = do_get(state, key)
      do_put(state, key, new_value, 0)
      old
    end
  end

  # Atomic GETDEL: reads value, deletes key, returns value directly (not
  # wrapped in {:ok, ...}). Returns nil if key does not exist.
  defp do_getdel(state, key) do
    with :ok <- ensure_string_key(state, key) do
      old = do_get(state, key)

      if old != nil do
        case do_delete(state, key) do
          :ok -> old
          {:error, _reason} = error -> error
        end
      else
        old
      end
    end
  end

  # Atomic GETEX: reads value, re-writes with new expire_at_ms, returns value
  # directly (not wrapped). Returns nil if key does not exist or is expired.
  defp do_getex(state, key, expire_at_ms) do
    with :ok <- ensure_string_key(state, key) do
      case do_get_meta(state, key) do
        nil ->
          nil

        {value, _old_exp} ->
          do_put(state, key, value, expire_at_ms)
          value
      end
    end
  end

  # Atomic SETRANGE: reads current value, pads with zero bytes if needed,
  # replaces bytes at offset, writes back. Preserves expire_at_ms.
  defp do_setrange(state, key, offset, value) do
    with :ok <- ensure_string_key(state, key) do
      {old_val, expire_at_ms} =
        case do_get_meta(state, key) do
          nil -> {"", 0}
          {v, exp} -> {to_disk_binary(v), exp}
        end

      new_val = sm_apply_setrange(old_val, offset, value)
      do_put(state, key, new_val, expire_at_ms)
      {:ok, byte_size(new_val)}
    end
  end

  # Atomic SETBIT: read bitmap, extend with zeros to include byte_index if
  # needed, flip the single bit, write back. Preserves expire_at_ms.
  # Returns the OLD bit at that offset (Redis semantics).
  defp do_setbit(state, key, offset, bit_val) do
    with :ok <- ensure_string_key(state, key) do
      {old_val, expire_at_ms} =
        case do_get_meta(state, key) do
          nil -> {<<>>, 0}
          {v, exp} -> {to_disk_binary(v), exp}
        end

      byte_index = div(offset, 8)
      bit_position = 7 - rem(offset, 8)

      extended =
        if byte_size(old_val) >= byte_index + 1 do
          old_val
        else
          old_val <> :binary.copy(<<0>>, byte_index + 1 - byte_size(old_val))
        end

      old_byte = :binary.at(extended, byte_index)
      old_bit = old_byte >>> bit_position &&& 1

      new_byte =
        case bit_val do
          1 -> old_byte ||| 1 <<< bit_position
          0 -> old_byte &&& bnot(1 <<< bit_position)
        end

      <<prefix::binary-size(byte_index), _old::8, suffix::binary>> = extended
      new_value = <<prefix::binary, new_byte::8, suffix::binary>>

      do_put(state, key, new_value, expire_at_ms)
      old_bit
    end
  end

  # Atomic HINCRBY: read compound key (H:<redis_key>\0<field>), parse integer,
  # add delta, write back. Returns new integer value, or {:error, reason}.
  defp do_hincrby(state, redis_key, field, delta) do
    compound_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, field)

    case sm_store_compound_get_meta(state, redis_key, compound_key) do
      nil ->
        if delta > @int64_max or delta < @int64_min do
          {:error, "ERR increment or decrement would overflow"}
        else
          do_put(state, compound_key, Integer.to_string(delta), 0)
          delta
        end

      {value, expire_at_ms} ->
        case coerce_integer(value) do
          {:ok, cur} ->
            new_val = cur + delta

            if new_val > @int64_max or new_val < @int64_min do
              {:error, "ERR increment or decrement would overflow"}
            else
              do_put(state, compound_key, Integer.to_string(new_val), expire_at_ms)
              new_val
            end

          :error ->
            {:error, "ERR hash value is not an integer"}
        end
    end
  end

  # Atomic HINCRBYFLOAT: same as HINCRBY but for floats.
  defp do_hincrbyfloat(state, redis_key, field, delta) do
    compound_key = Ferricstore.Store.CompoundKey.hash_field(redis_key, field)

    case sm_store_compound_get_meta(state, redis_key, compound_key) do
      nil ->
        new_val = delta * 1.0
        do_put(state, compound_key, Float.to_string(new_val), 0)
        Float.to_string(new_val)

      {value, expire_at_ms} ->
        case coerce_float(value) do
          {:ok, cur} ->
            new_val = cur + delta
            new_str = Float.to_string(new_val)
            do_put(state, compound_key, new_str, expire_at_ms)
            new_str

          :error ->
            {:error, "ERR hash value is not a valid float"}
        end
    end
  end

  # Atomic ZINCRBY: check/set type metadata, read member score, add delta,
  # write back. Returns the new score as a string. Returns {:error, ...} on
  # wrong type.
  defp do_zincrby(state, redis_key, increment, member) do
    type_key = Ferricstore.Store.CompoundKey.type_key(redis_key)
    expected_type = Ferricstore.Store.CompoundKey.encode_type(:zset)

    with :ok <- check_or_set_type(state, redis_key, type_key, expected_type) do
      compound_key = Ferricstore.Store.CompoundKey.zset_member(redis_key, member)

      current_score =
        case sm_store_compound_get_meta(state, redis_key, compound_key) do
          nil ->
            0.0

          {score_val, _expire_at_ms} ->
            score_str =
              case score_val do
                v when is_binary(v) -> v
                v -> to_string(v)
              end

            case Float.parse(score_str) do
              {s, ""} -> s
              _ -> 0.0
            end
        end

      new_score = current_score + increment * 1.0
      new_str = Float.to_string(new_score)

      do_put(state, compound_key, new_str, 0)
      zset_index_put(state, redis_key, compound_key, new_str)
      new_str
    end
  end

  defp do_spop(_state, _redis_key, count, _seed) when not (is_nil(count) or is_integer(count)),
    do: {:error, "ERR value is not an integer or out of range"}

  defp do_spop(_state, _redis_key, count, _seed) when is_integer(count) and count < 0,
    do: {:error, "ERR value is not an integer or out of range"}

  defp do_spop(state, redis_key, count, seed) do
    store = build_compound_store(state)

    with :ok <- Ferricstore.Store.TypeRegistry.check_type(redis_key, :set, store) do
      prefix = CompoundKey.set_prefix(redis_key)

      members =
        state
        |> shard_ets_state()
        |> Ferricstore.Store.Shard.ETS.prefix_scan_entries(prefix, state.shard_data_path)
        |> Enum.map(fn {member, _value} -> member end)
        |> Enum.sort()

      pop_count = if is_nil(count), do: 1, else: count

      # Raft apply must be deterministic on every replica. Use the committed
      # Ra index as the selection seed instead of caller-side randomness.
      selected = deterministic_take(members, pop_count, {redis_key, seed})

      selected_delete_keys = Enum.map(selected, &CompoundKey.set_member(redis_key, &1))

      with :ok <- do_compound_batch_delete(state, redis_key, selected_delete_keys),
           :ok <-
             maybe_delete_empty_compound_type_key_after_pop(
               state,
               redis_key,
               length(members),
               length(selected)
             ) do
        if is_nil(count), do: List.first(selected), else: selected
      end
    end
  end

  defp do_zpop(_state, _redis_key, count, _direction)
       when not is_integer(count) or count < 0,
       do: {:error, "ERR value is not an integer or out of range"}

  defp do_zpop(state, redis_key, count, direction) when direction in [:min, :max] do
    store = build_compound_store(state)

    with :ok <- Ferricstore.Store.TypeRegistry.check_type(redis_key, :zset, store) do
      prefix = CompoundKey.zset_prefix(redis_key)

      sorted =
        state
        |> shard_ets_state()
        |> Ferricstore.Store.Shard.ETS.prefix_scan_entries(prefix, state.shard_data_path)
        |> Enum.map(fn {member, score_value} ->
          {member, zpop_score(score_value)}
        end)
        |> Enum.sort_by(fn {member, score} -> {score, member} end)

      sorted = if direction == :max, do: Enum.reverse(sorted), else: sorted
      selected = Enum.take(sorted, count)

      selected_delete_keys =
        Enum.map(selected, fn {member, _score} -> CompoundKey.zset_member(redis_key, member) end)

      with :ok <- do_compound_batch_delete(state, redis_key, selected_delete_keys),
           :ok <-
             maybe_delete_empty_compound_type_key_after_pop(
               state,
               redis_key,
               length(sorted),
               length(selected)
             ) do
        Enum.flat_map(selected, fn {member, score} -> [member, format_zset_score(score)] end)
      end
    end
  end

  defp do_zpop(_state, _redis_key, _count, _direction),
    do: {:error, "ERR syntax error"}

  defp deterministic_take(_members, 0, _seed), do: []
  defp deterministic_take([], _count, _seed), do: []

  defp deterministic_take(members, count, seed) do
    size = length(members)
    count = min(count, size)
    start = :erlang.phash2(seed, size)
    {left, right} = Enum.split(members, start)
    Enum.take(right ++ left, count)
  end

  defp zpop_score(score) when is_binary(score) do
    case Float.parse(score) do
      {parsed, ""} -> parsed
      _ -> 0.0
    end
  end

  defp zpop_score(score) when is_number(score), do: score * 1.0
  defp zpop_score(_score), do: 0.0

  defp format_zset_score(score) when is_float(score) do
    :erlang.float_to_binary(score, [:compact, decimals: 17])
  end

  defp sm_store_compound_get_meta(state, redis_key, compound_key) do
    case sm_store_compound_path_fun(state, redis_key, compound_key) do
      nil ->
        do_get_meta(state, compound_key)

      path_fun ->
        case sm_store_batch_get(state, [compound_key], path_fun) do
          [value] when is_binary(value) ->
            case :ets.lookup(state.ets, compound_key) do
              [{^compound_key, _ets_value, exp, _lfu, _fid, _off, _vsize}] -> {value, exp}
              _ -> nil
            end

          _ ->
            nil
        end
    end
  end

  defp sm_store_compound_path_fun(state, redis_key, compound_key_or_prefix) do
    case promoted_compound_path(state, redis_key, compound_key_or_prefix) do
      nil -> nil
      dedicated_path -> fn _state, fid -> sm_file_path_from_path(dedicated_path, fid) end
    end
  end

  defp sm_store_batch_get(state, keys, path_fun) do
    {local_results, cold_reads, remote_entries} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, [], []}, fn {key, index}, {results, cold, remote} ->
        sm_store_collect_batch_get(state, key, index, results, cold, remote)
      end)

    results = sm_store_read_cold_batch(state, local_results, Enum.reverse(cold_reads), path_fun)

    remote_entries
    |> Enum.reverse()
    |> sm_store_batch_remote_get(instance_ctx_for_state(state), results)
    |> values_for_indexes(keys)
  end

  defp sm_store_collect_batch_get(state, key, index, results, cold, remote) do
    case sm_pending_value_meta(key) do
      {:hit, value, _exp} ->
        {Map.put(results, index, value), cold, remote}

      :miss ->
        sm_store_collect_committed_batch_get(state, key, index, results, cold, remote)
    end
  end

  defp sm_store_collect_committed_batch_get(state, key, index, results, cold, remote) do
    now = apply_now_ms()

    case :ets.lookup(state.ets, key) do
      [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
        {Map.put(results, index, value), cold, remote}

      [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        {results, [{index, key, 0, fid, off} | cold], remote}

      [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        {Map.put(results, index, value), cold, remote}

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when exp > now and valid_cold_location(fid, off, vsize) ->
        {results, [{index, key, exp, fid, off} | cold], remote}

      [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
        track_keydir_binary_remove_known(state, key, value)
        :ets.delete(state.ets, key)
        {results, cold, [{index, key} | remote]}

      [] ->
        {results, cold, [{index, key} | remote]}
    end
  rescue
    ArgumentError ->
      {results, cold, [{index, key} | remote]}
  end

  defp sm_store_read_cold_batch(_state, results, [], _path_fun), do: results

  defp sm_store_read_cold_batch(state, results, cold_reads, path_fun) do
    locations =
      Enum.map(cold_reads, fn {_index, key, _exp, fid, off} ->
        {path_fun.(state, fid), off, key}
      end)

    values =
      locations
      |> Ferricstore.Store.ColdRead.pread_batch_keyed(@cold_read_timeout_ms)
      |> normalize_state_machine_batch_values(length(cold_reads))

    emit_state_machine_batch_cold_errors(cold_reads, values, fn {_index, _key, _exp, fid, _off} ->
      path_fun.(state, fid)
    end)

    materialized_values = materialize_state_machine_batch_values(state, values)

    cold_reads
    |> Enum.zip(values)
    |> Enum.zip(materialized_values)
    |> Enum.reduce(results, fn
      {{{index, key, exp, fid, off}, value}, materialized}, acc
      when is_binary(value) and value == materialized ->
        ets_value = value_for_ets(value, hot_cache_threshold(state))
        track_keydir_binary_warm(state, ets_value)
        :ets.insert(state.ets, {key, ets_value, exp, LFU.initial(), fid, off, byte_size(value)})
        Map.put(acc, index, value)

      {{{index, _key, _exp, _fid, _off}, value}, materialized}, acc
      when is_binary(value) and is_binary(materialized) ->
        Map.put(acc, index, materialized)

      {_read_value, _materialized}, acc ->
        acc
    end)
  end

  defp materialize_state_machine_batch_values(state, values) do
    ctx = blob_apply_ctx(state)
    threshold = BlobValue.threshold(ctx)

    if threshold > 0 do
      binary_values = Enum.filter(values, &is_binary/1)

      materialized =
        BlobValue.maybe_materialize_many(
          Map.get(ctx, :data_dir),
          state.shard_index,
          threshold,
          binary_values
        )

      {inflated, _remaining} =
        Enum.map_reduce(values, materialized, fn
          value, [{:ok, materialized_value} | rest] when is_binary(value) ->
            {materialized_value, rest}

          value, [{:error, reason} | rest] when is_binary(value) ->
            {{:error, {:blob_ref_unavailable, reason}}, rest}

          value, rest ->
            {value, rest}
        end)

      inflated
    else
      values
    end
  end

  defp sm_store_batch_remote_get([], _ctx, results), do: results
  defp sm_store_batch_remote_get(_entries, nil, results), do: results

  defp sm_store_batch_remote_get(entries, ctx, results) do
    remote_keys = Enum.map(entries, fn {_index, key} -> key end)
    remote_values = Router.batch_get(ctx, remote_keys)

    merge_indexed_values(results, entries, remote_values)
  end

  defp merge_indexed_values(results, entries, values) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce(results, fn {entry, value}, acc ->
      merge_indexed_value(acc, entry, value)
    end)
  end

  defp merge_indexed_value(acc, {index, _key}, value) when is_integer(index),
    do: Map.put(acc, index, value)

  defp merge_indexed_value(acc, {_key, index}, value), do: Map.put(acc, index, value)

  defp values_for_indexes(results, keys) do
    keys
    |> Enum.with_index()
    |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
  end

  defp instance_ctx_for_state(%{instance_ctx: %FerricStore.Instance{} = ctx}), do: ctx

  defp instance_ctx_for_state(%{instance_name: name}) when is_atom(name) do
    instance_ctx_by_name(name)
  end

  defp instance_ctx_for_state(_state), do: instance_ctx_by_name(:default)

  defp instance_ctx_by_name(name) do
    FerricStore.Instance.get(name)
  rescue
    _ -> nil
  catch
    :exit, _ -> nil
  end

  # Mirror of TypeRegistry.check_or_set but operates on state machine state.
  # Writes type metadata on first use, returns :ok or wrongtype error.
  defp check_or_set_type(state, redis_key, type_key, expected_type) do
    case do_get(state, type_key) do
      nil ->
        # No type metadata yet. Reject if the key already exists as a plain string.
        if ets_has?(state.ets, redis_key) do
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        else
          do_put(state, type_key, expected_type, 0)
          :ok
        end

      existing when is_binary(existing) and existing == expected_type ->
        :ok

      _other ->
        {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
    end
  end

  # Overwrites bytes at `offset` with `value`, zero-padding if the original
  # string is shorter than offset. Mirrors shard.ex apply_setrange/3.
  defp sm_apply_setrange(old, offset, value) do
    old_len = byte_size(old)
    val_len = byte_size(value)

    cond do
      val_len == 0 ->
        if offset > old_len do
          old <> :binary.copy(<<0>>, offset - old_len)
        else
          old
        end

      offset >= old_len ->
        padding = :binary.copy(<<0>>, offset - old_len)
        old <> padding <> value

      offset + val_len >= old_len ->
        binary_part(old, 0, offset) <> value

      true ->
        binary_part(old, 0, offset) <>
          value <>
          binary_part(old, offset + val_len, old_len - offset - val_len)
    end
  end

  # ---------------------------------------------------------------------------
  # Private: compare-and-swap
  # ---------------------------------------------------------------------------

  # Reads the current value from ETS (with Bitcask fallback), compares it
  # against `expected`. If match, writes `new_value` with optional TTL.
  # Returns 1 (swapped), 0 (mismatch), or nil (missing/expired).
  #
  # Replicates the exact shard.ex handle_cas_direct logic.
  # NOTE: The caller (shard.ex) pre-computes expire_at_ms as an absolute
  # timestamp before entering Raft to keep the state machine deterministic
  # (no System.os_time calls). So the 5th arg is already absolute, not relative.
  defp do_cas(state, key, expected, new_value, expire_at_ms) do
    case ets_lookup(state, key) do
      {:hit, ^expected, old_exp} ->
        expire = if expire_at_ms, do: expire_at_ms, else: old_exp
        do_put(state, key, new_value, expire)
        1

      {:hit, _other, _exp} ->
        0

      :expired ->
        nil

      :miss ->
        nil
    end
  end

  # ---------------------------------------------------------------------------
  # Private: distributed lock operations
  # ---------------------------------------------------------------------------

  # Acquires a lock. If the key doesn't exist, is expired, or is already held
  # by the same owner, sets {owner, ttl}. Returns :ok or {:error, reason}.
  #
  # Replicates the exact shard.ex handle_lock_direct logic.
  # NOTE: The caller (shard.ex) pre-computes expire_at_ms as an absolute
  # timestamp before entering Raft to keep the state machine deterministic.
  defp do_lock(state, key, owner, expire_at_ms) do
    case ets_lookup(state, key) do
      {:hit, ^owner, _exp} ->
        # Same owner -- re-acquire (idempotent)
        do_put(state, key, owner, expire_at_ms)
        :ok

      {:hit, _other, _exp} ->
        {:error, "DISTLOCK lock is held by another owner"}

      _ ->
        # Missing or expired -- acquire
        do_put(state, key, owner, expire_at_ms)
        :ok
    end
  end

  # Releases a lock. If the key exists and the owner matches, deletes the key.
  # Returns 1 on success, {:error, reason} on owner mismatch.
  #
  # Replicates the exact shard.ex handle_unlock_direct logic.
  defp do_unlock(state, key, owner) do
    case ets_lookup(state, key) do
      {:hit, ^owner, _exp} ->
        do_delete(state, key)
        1

      {:hit, _other, _exp} ->
        {:error, "DISTLOCK caller is not the lock owner"}

      _ ->
        # Missing or expired -- treat as already unlocked
        1
    end
  end

  # Extends a lock's TTL. If the key exists and the owner matches, updates
  # the TTL. Returns 1 on success, {:error, reason} on mismatch or missing.
  #
  # Replicates the exact shard.ex handle_extend_direct logic.
  # NOTE: The caller (shard.ex) pre-computes expire_at_ms as an absolute
  # timestamp before entering Raft to keep the state machine deterministic.
  defp do_extend(state, key, owner, expire_at_ms) do
    case ets_lookup(state, key) do
      {:hit, ^owner, _exp} ->
        do_put(state, key, owner, expire_at_ms)
        1

      {:hit, _other, _exp} ->
        {:error, "DISTLOCK caller is not the lock owner"}

      _ ->
        {:error, "DISTLOCK lock does not exist or has expired"}
    end
  end

  # ---------------------------------------------------------------------------
  # Private: cross-shard key locking (mini-percolator)
  # ---------------------------------------------------------------------------

  # Lock map is stored in a process dictionary key per shard. This avoids
  # adding a field to the Raft state struct (which would require migration).
  # The process dictionary persists across apply/3 calls because ra runs the
  # state machine in a dedicated process.

  # Locks all keys atomically. If any key is already locked by a different
  # owner (and not expired), rejects the entire batch.
  # Returns {new_state, result} — locks are persisted in Raft state.
  defp do_lock_keys(state, keys, owner_ref, expire_at_ms) do
    locks = Map.get(state, :cross_shard_locks, %{})
    now = apply_now_ms()

    conflict =
      Enum.find(keys, fn key ->
        case Map.get(locks, key) do
          nil -> false
          {^owner_ref, _exp} -> false
          {_other, exp} -> exp > now
        end
      end)

    if conflict do
      {state, {:error, :keys_locked}}
    else
      # Prune expired locks to prevent unbounded memory growth
      pruned = Map.reject(locks, fn {_k, {_ref, exp}} -> exp <= now end)

      new_locks =
        Enum.reduce(keys, pruned, fn key, acc ->
          Map.put(acc, key, {owner_ref, expire_at_ms})
        end)

      {Map.put(state, :cross_shard_locks, new_locks), :ok}
    end
  end

  # Unlocks keys owned by the given owner_ref.
  # Returns {new_state, :ok}.
  defp do_unlock_keys(state, keys, owner_ref) do
    locks = Map.get(state, :cross_shard_locks, %{})

    new_locks =
      Enum.reduce(keys, locks, fn key, acc ->
        case Map.get(acc, key) do
          {^owner_ref, _exp} -> Map.delete(acc, key)
          _ -> acc
        end
      end)

    {Map.put(state, :cross_shard_locks, new_locks), :ok}
  end

  # Checks whether a key is locked by someone other than owner_ref.
  defp check_key_lock(state, key, owner_ref) do
    locks = Map.get(state, :cross_shard_locks, %{})

    if map_size(locks) == 0 do
      :ok
    else
      now = apply_now_ms()

      case Map.get(locks, key) do
        nil -> :ok
        {^owner_ref, _exp} -> :ok
        {_other, exp} when exp <= now -> :ok
        {_other, _exp} -> {:error, :key_locked}
      end
    end
  end

  # Writes an intent record. Returns {new_state, :ok}.
  defp do_write_intent(state, owner_ref, intent_map) do
    intents = Map.get(state, :cross_shard_intents, %{})
    {Map.put(state, :cross_shard_intents, Map.put(intents, owner_ref, intent_map)), :ok}
  end

  # Deletes an intent record. Returns {new_state, :ok}.
  defp do_delete_intent(state, owner_ref) do
    intents = Map.get(state, :cross_shard_intents, %{})
    {Map.put(state, :cross_shard_intents, Map.delete(intents, owner_ref)), :ok}
  end

  # Returns all intent records.
  defp do_get_intents(state) do
    Map.get(state, :cross_shard_intents, %{})
  end

  # ---------------------------------------------------------------------------
  # Private: sliding window rate limiter
  # ---------------------------------------------------------------------------

  # Implements a sliding window rate limiter. Reads current counters from ETS,
  # rotates windows as needed, computes the effective count using a weighted
  # sliding window approximation, and updates the stored state.
  # Returns [status, count, remaining, ms_until_reset].
  #
  # Replicates the exact shard.ex handle_ratelimit_add_direct logic.
  defp do_ratelimit_add(state, key, window_ms, max, count, precomputed_now_ms) do
    now = precomputed_now_ms || apply_now_ms()

    {cur_count, cur_start, prv_count} =
      case ets_lookup(state, key) do
        {:hit, value, _exp} -> decode_ratelimit(value, now)
        _ -> {0, now, 0}
      end

    # Rotate windows
    {cur_count, cur_start, prv_count} =
      cond do
        now - cur_start >= window_ms * 2 -> {0, now, 0}
        now - cur_start >= window_ms -> {0, now, cur_count}
        true -> {cur_count, cur_start, prv_count}
      end

    # Compute effective count with sliding window approximation
    elapsed = now - cur_start
    weight = max(0.0, 1.0 - elapsed / window_ms)
    effective = cur_count + trunc(Float.round(prv_count * weight))
    expire_at_ms = cur_start + window_ms * 2

    {status, final_count, remaining, value} =
      if effective + count > max do
        value = encode_ratelimit(cur_count, cur_start, prv_count)
        {"denied", effective, max(0, max - effective), value}
      else
        new_cur = cur_count + count
        new_eff = effective + count
        value = encode_ratelimit(new_cur, cur_start, prv_count)
        {"allowed", new_eff, max(0, max - new_eff), value}
      end

    do_put(state, key, value, expire_at_ms)
    ms_until_reset = max(0, cur_start + window_ms - now)
    [status, final_count, remaining, ms_until_reset]
  end

  # Delegates to the shared ValueCodec to avoid duplication with shard.ex.
  defp encode_ratelimit(cur, start, prev), do: ValueCodec.encode_ratelimit(cur, start, prev)

  defp decode_ratelimit(value, fallback_start_ms),
    do: ValueCodec.decode_ratelimit(value, fallback_start_ms)

  # ---------------------------------------------------------------------------
  # Private: ETS lookup with expiry checking
  # ---------------------------------------------------------------------------

  # Reads a key from ETS, checking expiry. Falls back to Bitcask for cold
  # keys. Returns {:hit, value, expire_at_ms}, :expired, or :miss.
  # Mirrors the shard's `ets_lookup/2` logic with Bitcask fallback for
  # keys that may not yet be warmed into ETS.
  defp ets_lookup(state, key) do
    case sm_pending_value_meta(key) do
      {:hit, value, exp} ->
        {:hit, value, exp}

      :miss ->
        ets_lookup_committed(state, key)
    end
  end

  defp sm_pending_value_meta(key) do
    pending = Process.get(:sm_pending_values)

    case pending && Map.get(pending, key) do
      :deleted ->
        :miss

      {value, 0} ->
        {:hit, value, 0}

      {value, exp} ->
        if exp > apply_now_ms() do
          {:hit, value, exp}
        else
          Process.put(:sm_pending_values, Map.delete(pending, key))
          :miss
        end

      _ ->
        :miss
    end
  end

  defp ets_lookup_committed(state, key) do
    now = apply_now_ms()

    case :ets.lookup(state.ets, key) do
      [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
        {:hit, value, 0}

      [{^key, nil, 0, _lfu, _fid, _off, _vsize}] ->
        # Cold key -- try Bitcask
        warm_from_bitcask(state, key)

      [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        {:hit, value, exp}

      [{^key, nil, exp, _lfu, _fid, _off, _vsize}] when exp > now ->
        # Cold key with valid TTL -- try Bitcask
        warm_from_bitcask_with_exp(state, key, exp)

      [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
        track_keydir_binary_remove_known(state, key, value)
        :ets.delete(state.ets, key)
        :expired

      [] ->
        # ETS miss -- try Bitcask for keys not yet in keydir
        warm_from_bitcask(state, key)
    end
  end

  # v2: warms a cold key from disk using the location stored in the ETS
  # 7-tuple. If the key has a cold entry (value=nil, fid/off known), reads
  # the value via pread_at and updates ETS. For truly missing keys (not in
  # ETS at all after recover_keydir), returns :miss.
  defp warm_from_bitcask(state, key) do
    case :ets.lookup(state.ets, key) do
      [{^key, nil, _exp, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        warm_from_disk(state, key, 0, fid, off, vsize)

      [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
        track_keydir_binary_remove_known(state, key, nil)
        :ets.delete(state.ets, key)
        :miss

      _ ->
        # :pending fid or truly missing -- cannot warm from disk.
        :miss
    end
  end

  defp warm_from_bitcask_with_exp(state, key, exp) do
    case :ets.lookup(state.ets, key) do
      [{^key, nil, _exp, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        warm_from_disk(state, key, exp, fid, off, vsize)

      [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
        track_keydir_binary_remove_known(state, key, nil)
        :ets.delete(state.ets, key)
        :miss

      _ ->
        # :pending fid or truly missing -- cannot warm from disk.
        :miss
    end
  end

  # Reads a value from disk at the given file_id + offset, warms ETS, and
  # returns {:hit, value, expire_at_ms}.
  # Applies the hot_cache_max_value_size threshold when re-warming ETS.
  defp warm_from_disk(state, key, expire_at_ms, fid, off, vsize) do
    path = sm_file_path(state, fid)
    original_location = {fid, off, vsize}

    case read_cold_async(path, off, key) do
      {:ok, value} when is_binary(value) ->
        case materialize_cold_blob_value(state, value) do
          {:ok, ^value} ->
            v = value_for_ets(value, hot_cache_threshold(state))
            # Cold -> warm: previous ETS value was nil, only new value bytes matter.
            track_keydir_binary_warm(state, v)

            :ets.insert(
              state.ets,
              {key, v, expire_at_ms, LFU.initial(), fid, off, byte_size(value)}
            )

            {:hit, value, expire_at_ms}

          {:ok, materialized} ->
            {:hit, materialized, expire_at_ms}

          {:error, _reason} ->
            retry_warm_from_changed_cold_location(
              state,
              key,
              original_location,
              @cold_location_retry_attempts
            )
        end

      _ ->
        retry_warm_from_changed_cold_location(
          state,
          key,
          original_location,
          @cold_location_retry_attempts
        )
    end
  end

  defp retry_warm_from_changed_cold_location(_state, _key, _original_location, 0), do: :miss

  defp retry_warm_from_changed_cold_location(state, key, original_location, attempts_left) do
    maybe_run_cold_location_miss_hook()
    Process.sleep(@cold_location_retry_sleep_ms)
    now = apply_now_ms()

    case :ets.lookup(state.ets, key) do
      [{^key, value, exp, _lfu, _fid, _off, _vsize}]
      when value != nil and (exp == 0 or exp > now) ->
        {:hit, value, exp}

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when (exp == 0 or exp > now) and valid_cold_location(fid, off, vsize) ->
        if {fid, off, vsize} == original_location do
          retry_warm_from_changed_cold_location(state, key, original_location, attempts_left - 1)
        else
          warm_from_disk(state, key, exp, fid, off, vsize)
        end

      _ ->
        :miss
    end
  end

  defp maybe_run_cold_location_miss_hook do
    case Process.get(:ferricstore_state_machine_cold_location_miss_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  defp read_cold_async(path, offset, key) do
    Ferricstore.Store.ColdRead.pread_at(path, offset, key, @cold_read_timeout_ms)
  end

  defp materialize_cold_blob_value(state, value) do
    ctx = blob_apply_ctx(state)

    BlobValue.maybe_materialize(
      Map.get(ctx, :data_dir),
      state.shard_index,
      BlobValue.threshold(ctx),
      value
    )
  end

  # Returns the full file path for a log file within this shard's data dir.
  defp sm_file_path(state, file_id) do
    Path.join(
      state.shard_data_path,
      "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log"
    )
  end

  # ---------------------------------------------------------------------------
  # Private: list operations (read-modify-write via ListOps)
  # ---------------------------------------------------------------------------

  # Performs a complete read-modify-write for a list operation within a single
  # Raft apply. The get/put/delete closures operate directly on ETS and Bitcask
  # (the same stores available to the state machine) so the entire operation is
  # atomic from the Raft log's perspective.
  defp do_checked_list_op(state, key, operation) do
    store = build_compound_store(state)

    type_store =
      Map.put(store, :exists?, fn k ->
        live_key?(state, k)
      end)

    case ensure_list_type_for_operation(key, operation, type_store) do
      :ok -> ListOps.execute(key, store, operation)
      {:error, _} = err -> err
    end
  end

  defp do_checked_lmove(state, source, destination, from_dir, to_dir) do
    store = build_compound_store(state)

    with :ok <- Ferricstore.Store.TypeRegistry.check_type(source, :list, store) do
      case ListOps.read_meta(source, store) do
        nil ->
          nil

        {0, _, _} ->
          nil

        _meta ->
          with :ok <- Ferricstore.Store.TypeRegistry.check_or_set(destination, :list, store) do
            ListOps.execute_lmove(source, destination, store, from_dir, to_dir)
          end
      end
    end
  end

  defp ensure_list_type_for_operation(key, operation, store)

  defp ensure_list_type_for_operation(key, {:lpush, _elements}, store),
    do: Ferricstore.Store.TypeRegistry.check_or_set(key, :list, store)

  defp ensure_list_type_for_operation(key, {:rpush, _elements}, store),
    do: Ferricstore.Store.TypeRegistry.check_or_set(key, :list, store)

  defp ensure_list_type_for_operation(key, _operation, store),
    do: Ferricstore.Store.TypeRegistry.check_type(key, :list, store)

  defp build_string_value_store(state) do
    %{
      get: fn key -> do_get(state, key) end,
      get_meta: fn key -> do_get_meta(state, key) end,
      batch_get: fn keys -> Enum.map(keys, &do_get(state, &1)) end,
      put: fn key, value, expire_at_ms -> do_put(state, key, value, expire_at_ms) end,
      delete: fn key -> do_delete(state, key) end,
      exists?: fn key -> live_key?(state, key) end,
      compound_get: fn _redis_key, compound_key -> do_get(state, compound_key) end
    }
  end

  defp do_pfmerge(state, dest_key, source_sketches) when is_list(source_sketches) do
    source_keys =
      source_sketches
      |> Enum.with_index()
      |> Enum.map(fn {_sketch, idx} -> "\0__pfmerge_source__:" <> Integer.to_string(idx) end)

    source_values = Map.new(Enum.zip(source_keys, source_sketches))
    base_store = build_string_value_store(state)

    store = %{
      base_store
      | batch_get: fn keys ->
          Enum.map(keys, fn key ->
            Map.get(source_values, key) || do_get(state, key)
          end)
        end,
        compound_get: fn redis_key, compound_key ->
          if Map.has_key?(source_values, redis_key) do
            nil
          else
            do_get(state, compound_key)
          end
        end
    }

    HyperLogLog.handle_ast({:pfmerge, [dest_key | source_keys]}, store)
  end

  # Builds a compound store for list/hash/set operations inside the state
  # machine. Uses do_put/do_delete/do_get directly (already inside apply context,
  # writes accumulate in pending_writes buffer for batch NIF flush).
  defp build_compound_store(state) do
    %{
      compound_get: fn _redis_key, compound_key ->
        do_get(state, compound_key)
      end,
      compound_put: fn _redis_key, compound_key, value, expire_at_ms ->
        do_put(state, compound_key, value, expire_at_ms)
      end,
      compound_batch_put: fn _redis_key, entries ->
        Enum.each(entries, fn {compound_key, value, expire_at_ms} ->
          :ok = do_put(state, compound_key, value, expire_at_ms)
        end)

        :ok
      end,
      compound_delete: fn _redis_key, compound_key ->
        do_delete(state, compound_key)
      end,
      compound_batch_delete: fn _redis_key, compound_keys ->
        Enum.reduce_while(compound_keys, :ok, fn compound_key, :ok ->
          case do_delete(state, compound_key) do
            :ok -> {:cont, :ok}
            {:error, _} = error -> {:halt, error}
          end
        end)
      end,
      compound_scan: fn _redis_key, prefix ->
        Ferricstore.Store.Shard.ETS.prefix_scan_entries(
          shard_ets_state(state),
          prefix,
          state.shard_data_path
        )
        |> Enum.sort_by(fn {field, _} -> field end)
      end,
      compound_count: fn _redis_key, prefix ->
        Ferricstore.Store.Shard.ETS.prefix_count_entries(shard_ets_state(state), prefix)
      end,
      exists?: fn key ->
        live_key?(state, key)
      end
    }
  end

  defp shard_ets_state(state) do
    %{keydir: state.ets, index: state.shard_index, instance_ctx: state.instance_ctx}
  end

  defp live_key?(state, key) do
    # Type checks use this for plain-key existence. Raw ETS presence is
    # incorrect because expired keys can stay unswept until the next read.
    case ets_lookup(state, key) do
      {:hit, _value, _expire_at_ms} -> true
      _ -> false
    end
  end

  # ---------------------------------------------------------------------------
  # Private: compound delete prefix (scan + batch delete)
  # ---------------------------------------------------------------------------

  # Scans ETS for all keys matching the given prefix and deletes each from
  # both ETS and Bitcask. Used by DEL on hashes, sets, and sorted sets to
  # remove all compound fields belonging to a data structure.
  #
  # Uses :ets.select with a match spec for O(matching) prefix lookup instead
  # of :ets.foldl which would scan every key in the entire keydir.
  defp do_delete_prefix(state, prefix) do
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    keys_to_delete = :ets.select(state.ets, match_spec)

    Enum.each(keys_to_delete, fn key ->
      do_delete(state, key)
    end)

    :ok
  end

  defp do_compound_put(state, redis_key, compound_key, value, expire_at_ms) do
    result =
      case promoted_compound_path(state, redis_key, compound_key) do
        nil ->
          do_put(state, compound_key, value, expire_at_ms)

        dedicated_path ->
          do_promoted_compound_put(
            state,
            redis_key,
            compound_key,
            value,
            expire_at_ms,
            dedicated_path
          )
      end

    if result == :ok do
      zset_index_put(state, redis_key, compound_key, value)
    end

    result
  end

  defp do_compound_batch_put(_state, _redis_key, []), do: :ok

  defp do_compound_batch_put(state, redis_key, entries) do
    case compound_batch_put_target(state, redis_key, entries) do
      :shared ->
        do_shared_compound_batch_put_fast(state, redis_key, entries)

      {:promoted, dedicated_path} ->
        do_promoted_compound_batch_put(state, redis_key, entries, dedicated_path)

      :mixed ->
        do_compound_batch_put_generic(state, redis_key, entries)
    end
  end

  defp compound_batch_put_target(state, redis_key, [{compound_key, _value, _expire_at_ms} | rest]) do
    first_path = promoted_compound_path(state, redis_key, compound_key)

    if Enum.all?(rest, fn {key, _value, _expire_at_ms} ->
         promoted_compound_path(state, redis_key, key) == first_path
       end) do
      case first_path do
        nil -> :shared
        dedicated_path -> {:promoted, dedicated_path}
      end
    else
      :mixed
    end
  end

  defp do_compound_batch_put_generic(state, redis_key, entries) do
    Enum.reduce_while(entries, :ok, fn {compound_key, value, expire_at_ms}, :ok ->
      case do_compound_put(state, redis_key, compound_key, value, expire_at_ms) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  # Shared compound batches use the same publish-after-append contract as
  # put_batch: do not install visible ETS rows until Bitcask returns ordered
  # locations for the whole batch. ZSET side indexes are queued and flushed
  # only after the append succeeds.
  defp do_shared_compound_batch_put_fast(state, redis_key, entries) do
    if compound_shared_fast_path?(state) do
      pending = Process.get(:sm_pending_writes, [])
      pending_values = Process.get(:sm_pending_values, %{})
      fast_publish? = fast_put_publish_possible?(pending, pending_values)

      pending =
        Enum.reduce(entries, pending, fn {compound_key, value, expire_at_ms}, acc ->
          disk_val = to_disk_binary(value)
          queue_zset_index_put_after_flush(state, redis_key, compound_key, disk_val)
          [{:put, compound_key, disk_val, expire_at_ms} | acc]
        end)

      Process.put(:sm_pending_writes, pending)
      Process.put(:sm_pending_fast_put_batch, fast_publish?)
      :ok
    else
      do_compound_batch_put_generic(state, redis_key, entries)
    end
  end

  defp compound_shared_fast_path?(_state) do
    not cross_shard_pending_active?() and not standalone_staged_apply?()
  end

  defp do_promoted_compound_batch_put(state, redis_key, entries, dedicated_path) do
    Promotion.await_compaction_latch(state, redis_key)

    active = Promotion.find_active(dedicated_path)
    fid = parse_fid_from_path(active)

    disk_entries =
      Enum.map(entries, fn {compound_key, value, expire_at_ms} ->
        {compound_key, to_disk_binary(value), expire_at_ms}
      end)

    case NIF.v2_append_batch(active, disk_entries) do
      {:ok, locations} when length(locations) == length(entries) ->
        entries
        |> Enum.zip(disk_entries)
        |> Enum.zip(locations)
        |> Enum.each(fn {{{compound_key, value, expire_at_ms}, {_key, disk_val, _exp}},
                         {offset, value_size}} ->
          ets_val = value_for_ets(value, hot_cache_threshold(state))
          track_keydir_binary_delta(state, compound_key, ets_val, expire_at_ms)

          :ets.insert(
            state.ets,
            {compound_key, ets_val, expire_at_ms, LFU.initial(), fid, offset, value_size}
          )

          sm_tx_put_pending(compound_key, value, expire_at_ms)

          deleted = Process.get(:tx_deleted_keys, MapSet.new())

          if MapSet.member?(deleted, compound_key) do
            Process.put(:tx_deleted_keys, MapSet.delete(deleted, compound_key))
          end

          zset_index_put(state, redis_key, compound_key, disk_val)
        end)

        :ok

      {:ok, locations} ->
        {:error, {:batch_result_mismatch, length(entries), locations}}

      {:error, _reason} = error ->
        error
    end
  end

  defp do_compound_delete(state, redis_key, compound_key) do
    result =
      case promoted_compound_path(state, redis_key, compound_key) do
        nil ->
          do_delete(state, compound_key)

        dedicated_path ->
          do_promoted_compound_delete(state, redis_key, compound_key, dedicated_path)
      end

    if result == :ok do
      zset_index_delete(state, redis_key, compound_key)
    end

    result
  end

  defp do_compound_batch_delete(_state, _redis_key, []), do: :ok

  defp do_compound_batch_delete(state, redis_key, compound_keys) do
    compound_keys
    |> Enum.chunk_by(&promoted_compound_path(state, redis_key, &1))
    |> Enum.reduce_while(:ok, fn keys, :ok ->
      result =
        case promoted_compound_path(state, redis_key, hd(keys)) do
          nil ->
            do_shared_compound_batch_delete(state, redis_key, keys)

          dedicated_path ->
            do_promoted_compound_batch_delete(state, redis_key, keys, dedicated_path)
        end

      case result do
        :ok -> {:cont, :ok}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp do_shared_compound_batch_delete(state, redis_key, compound_keys) do
    case do_shared_compound_batch_delete_fast(state, redis_key, compound_keys) do
      :fallback ->
        do_shared_compound_batch_delete_generic(state, redis_key, compound_keys)

      result ->
        result
    end
  end

  defp do_shared_compound_batch_delete_fast(state, redis_key, compound_keys) do
    with true <- compound_shared_fast_path?(state),
         {:ok, prepared} <- maybe_prepare_delete_batch_fast(state, compound_keys),
         true <- Process.get(:sm_pending_writes, []) == [],
         true <- Process.get(:sm_pending_values, %{}) == %{} do
      Enum.each(Enum.reverse(prepared), fn {compound_key, _prob_path} ->
        queue_zset_index_delete_after_flush(state, redis_key, compound_key)
        queue_pending_delete_fast(compound_key, nil)
      end)

      Process.put(:sm_pending_fast_delete_batch, true)
      :ok
    else
      _ -> :fallback
    end
  end

  defp do_shared_compound_batch_delete_generic(state, redis_key, compound_keys) do
    Enum.reduce_while(compound_keys, :ok, fn compound_key, :ok ->
      case do_delete(state, compound_key) do
        :ok ->
          zset_index_delete(state, redis_key, compound_key)
          {:cont, :ok}

        {:error, _} = error ->
          {:halt, error}
      end
    end)
  end

  defp do_promoted_compound_batch_delete(state, redis_key, compound_keys, dedicated_path) do
    Promotion.await_compaction_latch(state, redis_key)

    active = Promotion.find_active(dedicated_path)
    ops = Enum.map(compound_keys, &{:delete, &1})

    case NIF.v2_append_ops_batch_nosync(active, ops) do
      {:ok, locations} ->
        with :ok <- validate_promoted_tombstone_batch(locations, length(compound_keys)),
             :ok <- NIF.v2_fsync(active) do
          deleted =
            Enum.reduce(compound_keys, Process.get(:tx_deleted_keys, MapSet.new()), fn
              compound_key, acc ->
                track_keydir_binary_remove(state, compound_key)
                :ets.delete(state.ets, compound_key)
                sm_tx_drop_pending(compound_key)
                zset_index_delete(state, redis_key, compound_key)
                MapSet.put(acc, compound_key)
            end)

          Process.put(:tx_deleted_keys, deleted)
          :ok
        end

      {:error, _reason} = err ->
        err
    end
  end

  defp validate_promoted_tombstone_batch(locations, expected_count)
       when length(locations) == expected_count do
    if Enum.all?(locations, &valid_promoted_tombstone_location?/1) do
      :ok
    else
      {:error, {:tombstone_batch_result_mismatch, expected_count, locations}}
    end
  end

  defp validate_promoted_tombstone_batch(locations, expected_count),
    do: {:error, {:tombstone_batch_result_mismatch, expected_count, locations}}

  defp valid_promoted_tombstone_location?({:delete, offset, record_size})
       when is_integer(offset) and offset >= 0 and is_integer(record_size) and record_size >= 0,
       do: true

  defp valid_promoted_tombstone_location?(_location), do: false

  defp maybe_delete_empty_compound_type_key_after_pop(
         _state,
         _redis_key,
         _total_member_count,
         0
       ),
       do: :ok

  defp maybe_delete_empty_compound_type_key_after_pop(
         state,
         redis_key,
         total_member_count,
         selected_count
       ) do
    if selected_count >= total_member_count do
      do_compound_delete(state, redis_key, CompoundKey.type_key(redis_key))
    else
      :ok
    end
  end

  defp do_compound_delete_prefix(state, redis_key, prefix) do
    result =
      case promoted_compound_path(state, redis_key, prefix) do
        nil ->
          do_delete_prefix(state, prefix)

        _dedicated_path ->
          Promotion.await_compaction_latch(state, redis_key)
          delete_compound_prefix_from_ets(state, prefix)

          Promotion.cleanup_promoted!(
            redis_key,
            state.shard_data_path,
            state.ets,
            state.data_dir,
            state.shard_index,
            Map.get(state, :instance_ctx) || Map.get(state, :instance_name)
          )
      end

    if result == :ok do
      zset_index_clear(state, redis_key)
    end

    result
  end

  defp do_promoted_compound_put(
         state,
         redis_key,
         compound_key,
         value,
         expire_at_ms,
         dedicated_path
       ) do
    Promotion.await_compaction_latch(state, redis_key)

    value_for = value_for_ets(value, hot_cache_threshold(state))
    disk_val = to_disk_binary(value)
    active = Promotion.find_active(dedicated_path)
    fid = parse_fid_from_path(active)

    case NIF.v2_append_record(active, compound_key, disk_val, expire_at_ms) do
      {:ok, {offset, _record_size}} ->
        value_size = byte_size(disk_val)

        track_keydir_binary_delta(state, compound_key, value_for, expire_at_ms)

        :ets.insert(
          state.ets,
          {compound_key, value_for, expire_at_ms, LFU.initial(), fid, offset, value_size}
        )

        sm_tx_put_pending(compound_key, value, expire_at_ms)
        deleted = Process.get(:tx_deleted_keys, MapSet.new())

        if MapSet.member?(deleted, compound_key) do
          Process.put(:tx_deleted_keys, MapSet.delete(deleted, compound_key))
        end

        :ok

      {:error, _reason} = err ->
        err
    end
  end

  defp do_promoted_compound_delete(state, redis_key, compound_key, dedicated_path) do
    Promotion.await_compaction_latch(state, redis_key)

    track_keydir_binary_remove(state, compound_key)
    active = Promotion.find_active(dedicated_path)

    case NIF.v2_append_tombstone(active, compound_key) do
      {:ok, _offset} ->
        :ets.delete(state.ets, compound_key)
        sm_tx_drop_pending(compound_key)
        deleted = Process.get(:tx_deleted_keys, MapSet.new())
        Process.put(:tx_deleted_keys, MapSet.put(deleted, compound_key))
        :ok

      {:error, _reason} = err ->
        err
    end
  end

  defp zset_index_put(
         %{zset_score_index_name: index, zset_score_lookup_name: lookup},
         redis_key,
         key,
         value
       )
       when index != nil and lookup != nil do
    if standalone_staged_apply?() do
      queue_pending_zset_index_op({:put, index, lookup, redis_key, key, to_disk_binary(value)})
    else
      apply_zset_index_put(index, lookup, redis_key, key, to_disk_binary(value))
    end

    :ok
  end

  defp zset_index_put(_state, _redis_key, _key, _value), do: :ok

  defp zset_index_delete(
         %{zset_score_index_name: index, zset_score_lookup_name: lookup},
         redis_key,
         key
       )
       when index != nil and lookup != nil do
    if standalone_staged_apply?() do
      queue_pending_zset_index_op({:delete, index, lookup, redis_key, key})
    else
      apply_zset_index_delete(index, lookup, redis_key, key)
    end

    :ok
  end

  defp zset_index_delete(_state, _redis_key, _key), do: :ok

  defp queue_zset_index_put_after_flush(
         %{zset_score_index_name: index, zset_score_lookup_name: lookup},
         redis_key,
         key,
         value
       )
       when index != nil and lookup != nil do
    queue_pending_zset_index_op({:put, index, lookup, redis_key, key, to_disk_binary(value)})
  end

  defp queue_zset_index_put_after_flush(_state, _redis_key, _key, _value), do: :ok

  defp queue_zset_index_delete_after_flush(
         %{zset_score_index_name: index, zset_score_lookup_name: lookup},
         redis_key,
         key
       )
       when index != nil and lookup != nil do
    queue_pending_zset_index_op({:delete, index, lookup, redis_key, key})
  end

  defp queue_zset_index_delete_after_flush(_state, _redis_key, _key), do: :ok

  defp zset_index_clear(
         %{zset_score_index_name: index, zset_score_lookup_name: lookup},
         redis_key
       )
       when index != nil and lookup != nil do
    if standalone_staged_apply?() do
      queue_pending_zset_index_op({:clear, index, lookup, redis_key})
    else
      apply_zset_index_clear(index, lookup, redis_key)
    end

    :ok
  end

  defp zset_index_clear(_state, _redis_key), do: :ok

  defp queue_pending_zset_index_op(op) do
    pending = Process.get(:sm_pending_zset_index_ops, [])
    Process.put(:sm_pending_zset_index_ops, [op | pending])
  end

  defp flush_pending_zset_indexes(_state) do
    case Process.put(:sm_pending_zset_index_ops, []) do
      [] ->
        :ok

      pending when is_list(pending) ->
        pending
        |> Enum.reverse()
        |> Enum.each(&apply_pending_zset_index_op/1)

        :ok
    end
  end

  defp apply_pending_zset_index_op({:put, index, lookup, redis_key, key, value}) do
    apply_zset_index_put(index, lookup, redis_key, key, value)
  end

  defp apply_pending_zset_index_op({:delete, index, lookup, redis_key, key}) do
    apply_zset_index_delete(index, lookup, redis_key, key)
  end

  defp apply_pending_zset_index_op({:clear, index, lookup, redis_key}) do
    apply_zset_index_clear(index, lookup, redis_key)
  end

  defp apply_zset_index_put(index, lookup, redis_key, key, value) do
    if zset_index_tables?(%{zset_score_index_name: index, zset_score_lookup_name: lookup}) do
      ZSetIndex.apply_put_to_tables(index, lookup, redis_key, key, value)
    end
  end

  defp apply_zset_index_delete(index, lookup, redis_key, key) do
    if zset_index_tables?(%{zset_score_index_name: index, zset_score_lookup_name: lookup}) do
      ZSetIndex.apply_delete_to_tables(index, lookup, redis_key, key)
    end
  end

  defp apply_zset_index_clear(index, lookup, redis_key) do
    if zset_index_tables?(%{zset_score_index_name: index, zset_score_lookup_name: lookup}) do
      ZSetIndex.clear_key(index, lookup, redis_key)
    end
  end

  defp delete_compound_prefix_from_ets(state, prefix) do
    prefix_len = byte_size(prefix)

    match_spec = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    state.ets
    |> :ets.select(match_spec)
    |> Enum.each(fn key ->
      track_keydir_binary_remove(state, key)
      :ets.delete(state.ets, key)
      sm_tx_drop_pending(key)
      deleted = Process.get(:tx_deleted_keys, MapSet.new())
      Process.put(:tx_deleted_keys, MapSet.put(deleted, key))
    end)

    :ok
  end

  defp maybe_clear_compound_data_structure_for_string_put(state, key) do
    unless Ferricstore.Store.CompoundKey.internal_key?(key) do
      type_key = Ferricstore.Store.CompoundKey.type_key(key)

      case string_put_compound_marker(state, type_key) do
        nil ->
          :ok

        type ->
          clear_compound_prefix_for_string_put(state, key, type)
          do_delete(state, type_key)
      end
    end

    :ok
  end

  defp string_put_compound_marker(state, type_key) do
    case sm_pending_value_meta(type_key) do
      {:hit, type, _exp} ->
        type

      :miss ->
        case :ets.lookup(state.ets, type_key) do
          [] ->
            nil

          [{^type_key, type, 0, _lfu, _fid, _off, _vsize}] when type != nil ->
            type

          _entry ->
            do_get(state, type_key)
        end
    end
  end

  defp compound_data_structure_key?(state, key) do
    not Ferricstore.Store.CompoundKey.internal_key?(key) and
      do_get(state, Ferricstore.Store.CompoundKey.type_key(key)) != nil
  end

  defp ensure_string_key(state, key) do
    if compound_data_structure_key?(state, key),
      do: {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"},
      else: :ok
  end

  defp clear_compound_prefix_for_string_put(state, key, "hash"),
    do: do_delete_prefix(state, Ferricstore.Store.CompoundKey.hash_prefix(key))

  defp clear_compound_prefix_for_string_put(state, key, "list") do
    do_delete_prefix(state, Ferricstore.Store.CompoundKey.list_prefix(key))
    do_delete(state, Ferricstore.Store.CompoundKey.list_meta_key(key))
  end

  defp clear_compound_prefix_for_string_put(state, key, "set"),
    do: do_delete_prefix(state, Ferricstore.Store.CompoundKey.set_prefix(key))

  defp clear_compound_prefix_for_string_put(state, key, "zset"),
    do: do_delete_prefix(state, Ferricstore.Store.CompoundKey.zset_prefix(key))

  defp clear_compound_prefix_for_string_put(_state, _key, _type), do: :ok

  # ---------------------------------------------------------------------------
  # Private: read from ETS with Bitcask fallback
  # ---------------------------------------------------------------------------

  # Reads a value from ETS, falling back to Bitcask for cold keys. Mirrors
  # the shard's `do_get/2` logic so that list operations can read current
  # state within the state machine.
  defp do_get(state, key) do
    case ets_lookup(state, key) do
      {:hit, value, _exp} -> value
      :expired -> nil
      :miss -> nil
    end
  end

  # Reads a value + expire_at_ms from ETS, falling back to Bitcask for cold
  # keys. Returns `{value, expire_at_ms}` or `nil`.
  defp do_get_meta(state, key) do
    case ets_lookup(state, key) do
      {:hit, value, exp} -> {value, exp}
      :expired -> nil
      :miss -> nil
    end
  end

  # ---------------------------------------------------------------------------
  # Private: HLC merging (spec 2G.6)
  # ---------------------------------------------------------------------------

  # Merges a remote HLC timestamp into the local node's HLC. This is a
  # side-effect that does not affect the deterministic state machine output.
  #
  # The merge is wrapped in a try/catch because the HLC GenServer may not be
  # running in unit tests that exercise the state machine in isolation.
  @spec merge_hlc(HLC.timestamp()) :: :ok
  defp merge_hlc(remote_ts) do
    HLC.update(remote_ts)
  rescue
    # HLC GenServer not running (e.g. unit tests without full app)
    _error -> :ok
  catch
    :exit, _reason -> :ok
  end

  # ---------------------------------------------------------------------------
  # Private: keydir binary memory tracking
  # ---------------------------------------------------------------------------

  # Tracks off-heap binary bytes when inserting/updating a key in ETS.
  # Computes delta: new_bytes - old_bytes (if key existed before).
  defp track_keydir_binary_delta(state, key, new_ets_val, new_expire_at_ms) do
    previous = :ets.lookup(state.ets, key)
    track_keydir_binary_delta_from_previous(state, key, previous, new_ets_val, new_expire_at_ms)
  end

  defp track_keydir_binary_delta_from_previous(
         state,
         key,
         previous,
         new_ets_val,
         new_expire_at_ms
       ) do
    ref = keydir_binary_ref(state)

    ExpiryTracker.adjust(
      expiry_instance_ctx(state),
      state.shard_index,
      ExpiryTracker.entry_expire_at(previous),
      new_expire_at_ms
    )

    if ref do
      new_bytes = binary_byte_size(key) + binary_byte_size(new_ets_val)

      old_bytes =
        case previous do
          [{^key, old_val, _, _, _, _, _}] ->
            binary_byte_size(key) + binary_byte_size(old_val)

          _ ->
            0
        end

      delta = new_bytes - old_bytes
      if delta != 0, do: :atomics.add(ref, state.shard_index + 1, delta)
    end
  end

  # Tracks off-heap binary bytes when deleting a key from ETS.
  defp track_keydir_binary_remove(state, key) do
    ref = keydir_binary_ref(state)
    previous = :ets.lookup(state.ets, key)

    ExpiryTracker.adjust(
      expiry_instance_ctx(state),
      state.shard_index,
      ExpiryTracker.entry_expire_at(previous),
      0
    )

    if ref do
      bytes =
        case previous do
          [{^key, val, _, _, _, _, _}] ->
            binary_byte_size(key) + binary_byte_size(val)

          _ ->
            0
        end

      if bytes > 0, do: :atomics.sub(ref, state.shard_index + 1, bytes)
    end
  end

  # Tracks off-heap binary bytes when deleting a key whose value is already known.
  defp track_keydir_binary_remove_known(state, key, value) do
    ref = keydir_binary_ref(state)
    previous = :ets.lookup(state.ets, key)

    ExpiryTracker.adjust(
      expiry_instance_ctx(state),
      state.shard_index,
      ExpiryTracker.entry_expire_at(previous),
      0
    )

    if ref do
      bytes = binary_byte_size(key) + binary_byte_size(value)
      if bytes > 0, do: :atomics.sub(ref, state.shard_index + 1, bytes)
    end
  end

  # Tracks off-heap binary bytes when warming a cold key (nil -> value).
  defp track_keydir_binary_warm(state, new_ets_val) do
    ref = keydir_binary_ref(state)

    if ref do
      new_bytes = binary_byte_size(new_ets_val)
      if new_bytes > 0, do: :atomics.add(ref, state.shard_index + 1, new_bytes)
    end
  end

  defp keydir_binary_ref(%{instance_ctx: %{keydir_binary_bytes: ref, shard_count: count}} = state)
       when ref != nil do
    shard_index = metrics_shard_index(state)
    if shard_index < count, do: ref, else: nil
  end

  defp keydir_binary_ref(%{instance_name: name} = state) when is_atom(name) do
    keydir_binary_ref_for_instance(name, metrics_shard_index(state))
  end

  defp keydir_binary_ref(state) do
    keydir_binary_ref_for_instance(:default, metrics_shard_index(state))
  end

  defp expiry_instance_ctx(%{instance_ctx: %FerricStore.Instance{} = ctx}), do: ctx

  defp expiry_instance_ctx(%{instance_name: name}) when is_atom(name) do
    try do
      FerricStore.Instance.get(name)
    rescue
      _ -> nil
    catch
      :exit, _ -> nil
    end
  end

  defp expiry_instance_ctx(_state), do: nil

  defp metrics_shard_index(%{shard_index: shard_index}), do: shard_index
  defp metrics_shard_index(%{index: index}), do: index

  defp keydir_binary_ref_for_instance(name, shard_index) do
    try do
      %{keydir_binary_bytes: ref, shard_count: count} = FerricStore.Instance.get(name)
      if ref != nil and shard_index < count, do: ref, else: nil
    rescue
      _ -> nil
    catch
      :exit, _ -> nil
    end
  end

  defp binary_byte_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp binary_byte_size(_), do: 0

  # ---------------------------------------------------------------------------
  # Private: probabilistic data structure helpers
  # ---------------------------------------------------------------------------

  # Shorthand for the common prob command pattern: bump applied count +
  # maybe release cursor.
  defp bump_applied(meta, state, result) do
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  # Prob commands don't write to Bitcask log (they write to their own files),
  # so they use with_pending_writes to ensure any metadata puts are batched.
  defp do_prob_command(state, fun) do
    if Process.get(:sm_pending_writes, :undefined) == :undefined do
      with_pending_writes(state, fun)
    else
      fun.()
    end
  end

  # Returns the file path for a probabilistic data structure file.
  # Uses Base64 URL-safe encoding to handle arbitrary key bytes.
  defp prob_path(state, key, ext) do
    safe = Base.url_encode64(key, padding: false)
    prob_dir = prob_dir(state)
    Path.join(prob_dir, "#{safe}.#{ext}")
  end

  defp cms_source_paths(state, src_keys) do
    Enum.map(src_keys, &prob_path_for_key(state, &1, "cms"))
  end

  defp prob_path_for_key(state, key, ext) do
    safe = Base.url_encode64(key, padding: false)

    prob_dir =
      case instance_ctx_for_state(state) do
        %FerricStore.Instance{} = ctx ->
          idx = Router.shard_for(ctx, key)
          shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
          Path.join(shard_path, "prob")

        _ ->
          prob_dir(state)
      end

    Path.join(prob_dir, "#{safe}.#{ext}")
  end

  # Returns the prob directory for this shard.
  defp prob_dir(%{shard_data_path: shard_data_path}) do
    Path.join(shard_data_path, "prob")
  end

  # Ensures the prob directory exists. Fsyncs parent on first create
  # so the new dir's entry survives kernel panic.
  defp ensure_prob_dir(state) do
    dir = prob_dir(state)

    if Ferricstore.FS.exists?(dir) do
      :ok
    else
      Ferricstore.FS.mkdir_p!(dir)
      prob_fsync_dir(Path.dirname(dir), :create_prob_dir)
    end
  end

  # Called immediately after a `*_file_create` NIF to make the new
  # filename entry durable. The NIF already fsynced the file's data;
  # this fsyncs the directory so the entry itself is durable.
  defp prob_fsync_dir(state) do
    prob_fsync_dir(prob_dir(state), :prob_file_dir)
  end

  defp prob_fsync_dir(path, phase) do
    result =
      case Process.get(:ferricstore_prob_fsync_dir_hook) do
        fun when is_function(fun, 1) -> fun.(path)
        _ -> NIF.v2_fsync_dir(path)
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "StateMachine probabilistic directory fsync failed during #{phase} for #{path}: #{inspect(reason)}"
        )

        {:error, {:fsync_dir_failed, phase, reason}}
    end
  end

  defp create_bloom_metadata(state, key, num_bits, num_hashes, prob_meta) do
    path = prob_path(state, key, "bloom")

    with :ok <- ensure_prob_dir(state),
         :ok <-
           prob_create_and_fsync(state, path, NIF.bloom_file_create(path, num_bits, num_hashes)) do
      do_put(state, key, :erlang.term_to_binary(bloom_meta_with_path(prob_meta, path)), 0)
      :ok
    end
  end

  defp bloom_meta_with_path({:bloom_meta, meta}, path) when is_map(meta) do
    {:bloom_meta, Map.put(meta, :path, path)}
  end

  defp bloom_meta_with_path(_prob_meta, path), do: {:bloom_meta, %{path: path}}

  defp create_cms_metadata(state, key, width, depth) do
    path = prob_path(state, key, "cms")

    with :ok <- ensure_prob_dir(state),
         :ok <- prob_create_and_fsync(state, path, NIF.cms_file_create(path, width, depth)) do
      meta_val = {:cms_meta, %{width: width, depth: depth}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end
  end

  defp maybe_create_cms_merge_dst(state, dst_path, dst_key, create_params) do
    if Ferricstore.FS.exists?(dst_path) do
      :ok
    else
      %{width: width, depth: depth} = create_params

      with :ok <-
             prob_create_and_fsync(state, dst_path, NIF.cms_file_create(dst_path, width, depth)) do
        meta_val = {:cms_meta, %{width: width, depth: depth}}
        do_put(state, dst_key, :erlang.term_to_binary(meta_val), 0)
        :ok
      end
    end
  end

  defp create_cuckoo_metadata(state, key, capacity, bucket_size) do
    path = prob_path(state, key, "cuckoo")

    with :ok <- ensure_prob_dir(state),
         :ok <-
           prob_create_and_fsync(
             state,
             path,
             NIF.cuckoo_file_create(path, capacity, bucket_size)
           ) do
      meta_val = {:cuckoo_meta, %{capacity: capacity}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end
  end

  defp create_topk_metadata(state, key, k, width, depth, decay) do
    path = prob_path(state, key, "topk")

    with :ok <- ensure_prob_dir(state),
         :ok <-
           prob_create_and_fsync(
             state,
             path,
             NIF.topk_file_create_v2(path, k, width, depth, decay)
           ) do
      meta_val = {:topk_meta, %{path: path, k: k, width: width, depth: depth, decay: decay}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end
  end

  defp prob_create_and_fsync(state, path, create_result) do
    case normalize_prob_create_result(create_result) do
      :ok ->
        case normalize_prob_create_result(prob_fsync_dir(state)) do
          :ok ->
            record_pending_prob_create(path)
            :ok

          {:error, _reason} = error ->
            cleanup_created_prob_file(state, path)
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp record_pending_prob_create(path) when is_binary(path) do
    pending = Process.get(:sm_pending_prob_creates, [])
    Process.put(:sm_pending_prob_creates, [path | pending])
  end

  defp cleanup_created_prob_file(state, path) when is_binary(path) do
    try do
      case Ferricstore.FS.rm(path) do
        :ok ->
          _ = prob_fsync_dir(Path.dirname(path), :rollback_prob_file_create)
          :ok

        {:error, {:not_found, _}} ->
          :ok

        {:error, _reason} = error ->
          error
      end
    catch
      kind, reason -> {:error, {kind, reason}}
    end
    |> case do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "StateMachine probabilistic sidecar rollback failed for #{path}: #{inspect(reason)}"
        )

        emit_prob_sidecar_delete_failed(state, path, {:rollback_prob_file_create, reason})
        :ok
    end
  end

  defp normalize_prob_create_result(:ok), do: :ok
  defp normalize_prob_create_result({:ok, :ok}), do: :ok
  defp normalize_prob_create_result({:error, _reason} = error), do: error
  defp normalize_prob_create_result(other), do: {:error, {:unexpected_prob_nif_result, other}}

  # Auto-creates a bloom filter file if it doesn't exist.
  defp auto_create_bloom_if_needed(state, path, key, auto_create_params) do
    cond do
      Ferricstore.FS.exists?(path) ->
        :ok

      auto_create_params ->
        %{num_bits: nb, num_hashes: nh} = auto_create_params

        with :ok <- prob_create_and_fsync(state, path, NIF.bloom_file_create(path, nb, nh)) do
          meta_val = {:bloom_meta, Map.merge(auto_create_params, %{path: path})}
          do_put(state, key, :erlang.term_to_binary(meta_val), 0)
          :ok
        end

      true ->
        :ok
    end
  end

  # Applies a prob command locally (used in cross-shard tx context where
  # the state machine is already running inside Raft apply).
  defp apply_prob_locally(instance_ctx, command) do
    # In cross-shard tx, prob commands go through Router.prob_write
    # which routes to the correct shard's Raft group.
    Router.prob_write(instance_ctx, command)
  end

  # Auto-creates a cuckoo filter file if it doesn't exist.
  defp auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
    cond do
      Ferricstore.FS.exists?(path) ->
        :ok

      auto_create_params ->
        %{capacity: cap, bucket_size: bs} = auto_create_params

        with :ok <- prob_create_and_fsync(state, path, NIF.cuckoo_file_create(path, cap, bs)) do
          meta_val = {:cuckoo_meta, %{capacity: cap}}
          do_put(state, key, :erlang.term_to_binary(meta_val), 0)
          :ok
        end

      true ->
        :ok
    end
  end

  # Enhanced do_delete that cleans up prob files.
  # When a key's value is a prob metadata marker, delete the associated file.
  defp prob_file_path_for_delete(state, key) do
    case do_get(state, key) do
      nil ->
        nil

      value when is_binary(value) ->
        try do
          case :erlang.binary_to_term(value, [:safe]) do
            {:bloom_meta, %{path: path}} -> path
            {:cms_meta, _} -> prob_path(state, key, "cms")
            {:cuckoo_meta, _} -> prob_path(state, key, "cuckoo")
            {:topk_meta, %{path: path}} -> path
            _ -> nil
          end
        rescue
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp maybe_delete_prob_file_path(_state, nil), do: :ok

  defp maybe_delete_prob_file_path(state, path) do
    result =
      try do
        case Ferricstore.FS.rm(path) do
          :ok -> prob_fsync_dir(state)
          {:error, {:not_found, _}} -> :ok
          {:error, reason} -> {:error, {:delete_prob_file_failed, reason}}
          other -> {:error, {:unexpected_delete_prob_file_result, other}}
        end
      rescue
        error -> {:error, {:delete_prob_file_exception, error}}
      catch
        :exit, reason -> {:error, {:delete_prob_file_exit, reason}}
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "StateMachine probabilistic sidecar delete failed for #{path}: #{inspect(reason)}"
        )

        emit_prob_sidecar_delete_failed(state, path, reason)
        :ok
    end
  end

  defp emit_prob_sidecar_delete_failed(state, path, reason) do
    :telemetry.execute(
      [:ferricstore, :prob, :sidecar_delete_failed],
      %{count: 1},
      %{shard_index: state.shard_index, path: path, reason: reason}
    )
  end
end
