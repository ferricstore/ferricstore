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
  `:release_cursor_interval` applied commands (default: 20_000), `apply/3`
  emits `{:checkpoint, ra_index, state}` and `{:release_cursor, ra_index}`
  effects. This tells ra that all log entries up to `ra_index` are fully
  reflected in the given state checkpoint and can be safely truncated after
  the checkpoint is materialized.

  The interval is stored in the machine state at init time (from the config
  map or application env) so that `apply/3` remains deterministic -- it never
  reads runtime configuration.
  """

  @behaviour :ra_machine

  import Bitwise

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.HLC
  alias Ferricstore.Store.{BitcaskWriter, ColdRead, LFU, ListOps, Promotion, Router, ValueCodec}
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  @default_release_cursor_interval 20_000
  @default_max_active_file_size 256 * 1024 * 1024
  @default_fragmentation_threshold 0.5
  @default_dead_bytes_threshold 134_217_728
  @bitcask_record_header_size 26
  @cold_read_timeout_ms 10_000

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
      applied_count: 0,
      release_cursor_interval: interval,
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
    * `{:delete, key}` -- Delete a key. Writes a tombstone to Bitcask, removes
      from ETS.
    * `{:batch, commands}` -- Apply a list of commands atomically. Each command
      in the batch is a tuple matching one of the above forms. Returns
      `{:ok, results}` where results is a list of individual command results.
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
    __MODULE__.apply(meta, :erlang.binary_to_term(binary), state)
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

  def apply(meta, {:batch, commands}, state) do
    with_apply_time(meta, fn ->
      old_count = state.applied_count

      # All commands in a batch share one pending-writes buffer so they
      # are flushed in a single v2_append_batch_nosync NIF call.
      write_result =
        with_pending_writes(state, fn ->
          Enum.map_reduce(commands, old_count, fn cmd, count ->
            result = apply_single(state, cmd)
            {result, count + 1}
          end)
        end)

      case write_result do
        {:error, _reason} = error ->
          new_state = %{state | applied_count: old_count + length(commands)}
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
          Enum.reduce(shard_batches, %{}, fn {shard_idx, queue, sandbox_namespace}, acc ->
            store = build_cross_shard_store(shard_idx, state)

            Process.put(:tx_deleted_keys, MapSet.new())
            Process.put(:tx_pending_values, %{})

            results =
              try do
                Enum.map(queue, fn {cmd, args} ->
                  namespaced_args = namespace_args(args, sandbox_namespace)

                  try do
                    Dispatcher.dispatch(cmd, namespaced_args, store)
                  catch
                    :exit, {:noproc, _} ->
                      {:error, "ERR server not ready, shard process unavailable"}

                    :exit, {reason, _} ->
                      {:error, "ERR internal error: #{inspect(reason)}"}
                  end
                end)
              after
                Process.delete(:tx_deleted_keys)
                Process.delete(:tx_pending_values)
              end

            Map.put(acc, shard_idx, results)
          end)
        end)

      new_state = %{state | applied_count: old_count + 1}

      case write_result do
        {:error, _reason} = error -> maybe_release_cursor(meta, old_count, new_state, error)
        shard_results -> maybe_release_cursor(meta, old_count, new_state, shard_results)
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

  def apply(meta, {:getset, key, new_value}, state) do
    apply_pending_with_time(meta, state, fn -> do_getset(state, key, new_value) end)
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

  # Generic JSON RMW — dispatches to Ferricstore.Commands.Json.handle with
  # a state-machine-scoped store. Covers JSON.SET / JSON.DEL(path) /
  # JSON.NUMINCRBY / JSON.ARRAPPEND / JSON.ARRPOP / JSON.ARRINSERT /
  # JSON.STRAPPEND / JSON.TOGGLE / JSON.CLEAR / JSON.MERGE. Read-only
  # ops (JSON.GET, JSON.TYPE, etc.) still run in the caller; only RMW
  # goes through Raft.
  def apply(meta, {:json_op, cmd, args}, state) do
    apply_pending_with_time(meta, state, fn ->
      Ferricstore.Commands.Json.handle(cmd, args, build_sm_store(state))
    end)
  end

  # Atomic HyperLogLog RMW — PFADD reads HLL blob, merges hashed elements,
  # writes back. PFMERGE reads N sources + destination, writes merged blob.
  def apply(meta, {:hll_op, cmd, args}, state) do
    apply_pending_with_time(meta, state, fn ->
      Ferricstore.Commands.HyperLogLog.handle(cmd, args, build_sm_store(state))
    end)
  end

  # Generic bitmap RMW — covers BITOP (merge N sources into destination) and
  # BITFIELD (multi-subcommand RMW on one bitmap). SETBIT has its own apply
  # clause above for the hot-path single-bit case.
  def apply(meta, {:bitmap_op, cmd, args}, state) do
    apply_pending_with_time(meta, state, fn ->
      Ferricstore.Commands.Bitmap.handle(cmd, args, build_sm_store(state))
    end)
  end

  # Geo RMW — GEOADD merges members into the zset blob; GEOSEARCHSTORE
  # writes a filtered subset to destination. Read-only ops (GEOPOS / GEODIST
  # / GEOHASH / GEOSEARCH) don't go through this path.
  def apply(meta, {:geo_op, cmd, args}, state) do
    apply_pending_with_time(meta, state, fn ->
      Ferricstore.Commands.Geo.handle(cmd, args, build_sm_store(state))
    end)
  end

  # TDigest RMW — TDIGEST.ADD/RESET/MERGE/CREATE. The old latch-based
  # serialization (with_tdigest_latch in ferricstore.ex) had subtle races
  # with orphan cleanup; state-machine dispatch removes the latch entirely.
  def apply(meta, {:tdigest_op, cmd, args}, state) do
    apply_pending_with_time(meta, state, fn ->
      Ferricstore.Commands.TDigest.handle(cmd, args, build_sm_store(state))
    end)
  end

  def apply(meta, {:cas, key, expected, new_value, ttl_ms}, state) do
    apply_pending_with_time(meta, state, fn -> do_cas(state, key, expected, new_value, ttl_ms) end)
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
    {new_state, result} = do_unlock_keys(state, keys, owner_ref)
    old_count = state.applied_count
    new_state = %{new_state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:cross_shard_intent, owner_ref, intent_map}, state) do
    {new_state, result} = do_write_intent(state, owner_ref, intent_map)
    old_count = state.applied_count
    new_state = %{new_state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:delete_intent, owner_ref}, state) do
    {new_state, result} = do_delete_intent(state, owner_ref)
    old_count = state.applied_count
    new_state = %{new_state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:get_intents}, state) do
    result = do_get_intents(state)
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:get_lock_count}, state) do
    result = map_size(Map.get(state, :cross_shard_locks, %{}))
    old_count = state.applied_count
    new_state = %{state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, result)
  end

  def apply(meta, {:clear_locks}, state) do
    new_state = %{state | cross_shard_locks: %{}, cross_shard_intents: %{}}
    old_count = state.applied_count
    new_state = %{new_state | applied_count: old_count + 1}
    maybe_release_cursor(meta, old_count, new_state, :ok)
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
      ensure_prob_dir(state)

      case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
        :ok -> NIF.bloom_file_add(path, element)
        {:error, _reason} = error -> error
      end
    end)
  end

  def apply(meta, {:bloom_madd, key, elements, auto_create_params}, state) do
    apply_prob_with_time(meta, state, fn ->
      path = prob_path(state, key, "bloom")
      ensure_prob_dir(state)

      case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
        :ok -> NIF.bloom_file_madd(path, elements)
        {:error, _reason} = error -> error
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

  # src_paths are pre-resolved absolute paths (sources may be on different shards)
  def apply(meta, {:cms_merge, dst_key, src_paths, weights, create_params}, state) do
    apply_prob_with_time(meta, state, fn ->
      dst_path = prob_path(state, dst_key, "cms")
      ensure_prob_dir(state)

      case maybe_create_cms_merge_dst(state, dst_path, dst_key, create_params) do
        :ok -> NIF.cms_file_merge(dst_path, src_paths, weights)
        {:error, _reason} = error -> error
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
      ensure_prob_dir(state)

      case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
        :ok -> NIF.cuckoo_file_add(path, element)
        {:error, _reason} = error -> error
      end
    end)
  end

  def apply(meta, {:cuckoo_addnx, key, element, auto_create_params}, state) do
    apply_prob_with_time(meta, state, fn ->
      path = prob_path(state, key, "cuckoo")
      ensure_prob_dir(state)

      case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
        :ok -> NIF.cuckoo_file_addnx(path, element)
        {:error, _reason} = error -> error
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
    hook = raft_apply_hook(state)
    result = if hook, do: hook.(command), else: {:error, :no_hook}
    bump_applied(meta, state, result)
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

    if div(old_count, interval) != div(state.applied_count, interval) and
         checkpoint_clean?(state) do
      case Ferricstore.Raft.ReplaySafeIndex.persist(state.shard_data_path, ra_index) do
        :ok ->
          record_cursor_metric(state, :last_released_cursor_index, ra_index)

          # Checkpoints are cheap, non-fsynced materializations used to accelerate
          # unclean restart. The cursor effect promotes the latest completed
          # checkpoint to a durable snapshot for log compaction when one exists.
          {state, wrapped_result,
           [notify_effect, {:checkpoint, ra_index, state}, {:release_cursor, ra_index}]}

        {:error, _reason} ->
          {state, wrapped_result, [notify_effect]}
      end
    else
      {state, wrapped_result, [notify_effect]}
    end
  end

  defp maybe_release_cursor(_meta, _old_count, state, result) do
    state = consume_pending_state(state)

    # No meta (e.g. cross-shard sub-apply) — pass through untouched.
    {state, result}
  end

  defp consume_pending_state(state) do
    case Process.delete(:sm_pending_state) do
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

  # Unit tests and old default-state-machine callers may not carry an
  # Instance. Custom instances fail closed above because their checkpoint
  # atomics are isolated by instance and must be resolved before log release.
  defp checkpoint_clean?(%{instance_ctx: nil}), do: true

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
    Process.delete(:sm_pending_state)
    :ok
  end

  defp apply_now_ms do
    CommandTime.now_ms()
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
      result = with_pending_writes(state, fun)
      bump_applied(meta, state, result)
    end)
  end

  defp apply_prob_with_time(meta, state, fun) do
    with_apply_time(meta, fn ->
      result = do_prob_command(state, fun)
      bump_applied(meta, state, result)
    end)
  end

  # ---------------------------------------------------------------------------
  # Private: cross-shard transaction store builder
  # ---------------------------------------------------------------------------

  # Builds a store map for a given shard_idx, usable by Dispatcher.dispatch.
  # For the anchor shard (matching state.shard_index), uses state directly.
  # For remote shards, reads active file info from persistent_term.
  defp build_cross_shard_store(shard_idx, anchor_state) do
    instance_ctx = anchor_state.instance_ctx

    data_dir =
      if instance_ctx do
        instance_ctx.data_dir
      else
        anchor_state.data_dir
      end

    ctx =
      if shard_idx == anchor_state.shard_index do
        %{
          instance_ctx: instance_ctx,
          keydir: anchor_state.ets,
          index: shard_idx,
          data_dir: data_dir,
          shard_data_path: anchor_state.shard_data_path,
          active_file_path: anchor_state.active_file_path,
          active_file_id: anchor_state.active_file_id
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

        %{
          instance_ctx: instance_ctx,
          keydir: keydir,
          index: shard_idx,
          data_dir: data_dir,
          shard_data_path: shard_data_path,
          active_file_path: file_path,
          active_file_id: file_id
        }
      end

    tx_binary_ref = keydir_binary_ref(anchor_state)

    local_put = fn key, value, expire_at_ms ->
      value_for = value_for_ets(value, hot_cache_threshold(anchor_state))
      disk_val = to_disk_binary(value)

      record_cross_shard_pending_original(ctx, key)

      if tx_binary_ref do
        new_bytes = binary_byte_size(key) + binary_byte_size(value_for)

        old_bytes =
          case :ets.lookup(ctx.keydir, key) do
            [{^key, old_val, _, _, _, _, _}] -> binary_byte_size(key) + binary_byte_size(old_val)
            _ -> 0
          end

        delta = new_bytes - old_bytes
        if delta != 0, do: :atomics.add(tx_binary_ref, ctx.index + 1, delta)
      end

      :ets.insert(
        ctx.keydir,
        {key, value_for, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
      )

      sm_tx_put_pending(key, value, expire_at_ms)
      deleted = Process.get(:tx_deleted_keys, MapSet.new())

      if MapSet.member?(deleted, key) do
        Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
      end

      queue_cross_shard_pending_put(ctx, key, disk_val, expire_at_ms, value_for)

      :ok
    end

    local_delete = fn key ->
      record_cross_shard_pending_original(ctx, key)

      if tx_binary_ref do
        bytes =
          case :ets.lookup(ctx.keydir, key) do
            [{^key, val, _, _, _, _, _}] -> binary_byte_size(key) + binary_byte_size(val)
            _ -> 0
          end

        if bytes > 0, do: :atomics.sub(tx_binary_ref, ctx.index + 1, bytes)
      end

      :ets.delete(ctx.keydir, key)
      sm_tx_drop_pending(key)
      deleted = Process.get(:tx_deleted_keys, MapSet.new())
      Process.put(:tx_deleted_keys, MapSet.put(deleted, key))
      queue_cross_shard_pending_delete(ctx, key)
      :ok
    end

    local_get = fn key ->
      deleted = Process.get(:tx_deleted_keys, MapSet.new())

      if MapSet.member?(deleted, key) do
        nil
      else
        case sm_tx_pending_meta(key) do
          {value, _exp} -> value
          nil -> cross_shard_ets_read(ctx, key)
        end
      end
    end

    local_get_meta = fn key ->
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
      deleted = Process.get(:tx_deleted_keys, MapSet.new())

      if MapSet.member?(deleted, key) do
        false
      else
        sm_tx_pending_meta(key) != nil or cross_shard_ets_read(ctx, key) != nil
      end
    end

    local_incr = fn key, delta ->
      current = local_get.(key)

      case current do
        nil ->
          local_put.(key, delta, 0)
          {:ok, delta}

        value ->
          case coerce_integer(value) do
            {:ok, int_val} ->
              new_val = int_val + delta
              local_put.(key, new_val, 0)
              {:ok, new_val}

            :error ->
              {:error, "ERR value is not an integer or out of range"}
          end
      end
    end

    local_incr_float = fn key, delta ->
      current = local_get.(key)

      case current do
        nil ->
          new_val = delta * 1.0
          local_put.(key, new_val, 0)
          {:ok, new_val}

        value ->
          case coerce_float(value) do
            {:ok, float_val} ->
              new_val = float_val + delta
              local_put.(key, new_val, 0)
              {:ok, new_val}

            :error ->
              {:error, "ERR value is not a valid float"}
          end
      end
    end

    local_append = fn key, suffix ->
      current =
        case local_get.(key) do
          nil -> ""
          v when is_integer(v) -> Integer.to_string(v)
          v when is_float(v) -> Float.to_string(v)
          v -> v
        end

      new_val = current <> suffix
      local_put.(key, new_val, 0)
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
      old =
        case local_get.(key) do
          nil -> ""
          v when is_integer(v) -> Integer.to_string(v)
          v when is_float(v) -> Float.to_string(v)
          v -> v
        end

      new_val = sm_apply_setrange(old, offset, value)
      local_put.(key, new_val, 0)
      {:ok, byte_size(new_val)}
    end

    promoted_put = fn compound_key, value, expire_at_ms, dedicated_path ->
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

    promoted_delete = fn compound_key, dedicated_path ->
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

    %{
      get: local_get,
      get_meta: local_get_meta,
      batch_get: fn keys -> cross_shard_batch_read(ctx, keys) end,
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
        cross_shard_compound_read(ctx, redis_key, compound_key)
      end,
      compound_get_meta: fn redis_key, compound_key ->
        cross_shard_compound_read_meta(ctx, redis_key, compound_key)
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        cross_shard_compound_batch_read(ctx, redis_key, compound_keys)
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        cross_shard_compound_batch_read_meta(ctx, redis_key, compound_keys)
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        case promoted_compound_path(ctx, redis_key, compound_key) do
          nil -> local_put.(compound_key, value, expire_at_ms)
          dedicated_path -> promoted_put.(compound_key, value, expire_at_ms, dedicated_path)
        end
      end,
      compound_delete: fn redis_key, compound_key ->
        case promoted_compound_path(ctx, redis_key, compound_key) do
          nil -> local_delete.(compound_key)
          dedicated_path -> promoted_delete.(compound_key, dedicated_path)
        end
      end,
      compound_scan: fn redis_key, prefix ->
        cross_shard_compound_scan(ctx, redis_key, prefix)
      end,
      compound_count: fn _redis_key, prefix ->
        cross_shard_prefix_count(ctx, prefix)
      end,
      compound_delete_prefix: fn _redis_key, prefix ->
        cross_shard_delete_prefix(ctx, prefix, local_delete)
      end,
      prob_dir: fn ->
        Path.join(ctx.shard_data_path, "prob")
      end,
      prob_write: fn command ->
        # Within cross-shard tx, prob writes are applied directly
        # (the state machine is already applying through Raft)
        apply_prob_locally(instance_ctx, command)
      end,
      shard_index: ctx.index,
      data_dir: data_dir
    }
  end

  defp namespace_args(args, nil), do: args
  defp namespace_args([], _ns), do: []
  defp namespace_args([key | rest], ns) when is_binary(key), do: [ns <> key | rest]
  defp namespace_args(args, _ns), do: args

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

  defp cross_shard_ets_read_from_path(ctx, key, data_path) do
    now = apply_now_ms()

    try do
      case :ets.lookup(ctx.keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          value

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          path = sm_file_path_from_path(data_path, fid)

          case read_cold_async(path, off, key) do
            {:ok, v} -> v
            _ -> nil
          end

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          value

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          path = sm_file_path_from_path(data_path, fid)

          case read_cold_async(path, off, key) do
            {:ok, v} -> v
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

  defp cross_shard_ets_read_meta_from_path(ctx, key, data_path) do
    now = apply_now_ms()

    try do
      case :ets.lookup(ctx.keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          {value, 0}

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          path = sm_file_path_from_path(data_path, fid)

          case read_cold_async(path, off, key) do
            {:ok, v} -> {v, 0}
            _ -> nil
          end

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {value, exp}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          path = sm_file_path_from_path(data_path, fid)

          case read_cold_async(path, off, key) do
            {:ok, v} -> {v, exp}
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
      ArgumentError -> :ok
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

  # JSON ops: first arg is always the key (JSON.SET key path value, etc.).
  defp async_key_present?(state, {:json_op, _cmd, [key | _]}) do
    ets_has?(state.ets, key)
  end

  # HLL ops: first arg is always the key (PFADD key ..., PFMERGE dest ...).
  defp async_key_present?(state, {:hll_op, _cmd, [key | _]}) do
    ets_has?(state.ets, key)
  end

  # Bitmap ops: BITFIELD has the key as the first arg; BITOP has [op, destkey | srcs].
  defp async_key_present?(state, {:bitmap_op, "BITOP", [_op, destkey | _]}) do
    ets_has?(state.ets, destkey)
  end

  defp async_key_present?(state, {:bitmap_op, _cmd, [key | _]}) when is_binary(key) do
    ets_has?(state.ets, key)
  end

  # Geo ops: GEOADD has key first; GEOSEARCHSTORE has [dest, source | ...]
  # — dest is the written key.
  defp async_key_present?(state, {:geo_op, "GEOSEARCHSTORE", [dest | _]}) do
    ets_has?(state.ets, dest)
  end

  defp async_key_present?(state, {:geo_op, _cmd, [key | _]}) when is_binary(key) do
    ets_has?(state.ets, key)
  end

  # TDigest ops: first arg is always the key.
  defp async_key_present?(state, {:tdigest_op, _cmd, [key | _]}) when is_binary(key) do
    ets_has?(state.ets, key)
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
        :ok

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

  defp apply_single(state, {:put, key, value, expire_at_ms}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_put(state, key, value, expire_at_ms)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:set, key, value, expire_at_ms, opts}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_set(state, key, value, expire_at_ms, opts)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:delete, key}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_delete(state, key)
      {:error, :key_locked} -> {:error, :key_locked}
    end
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

  defp apply_single(state, {:compound_delete, compound_key}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(compound_key)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_compound_delete(state, redis_key, compound_key)
      {:error, :key_locked} -> {:error, :key_locked}
    end
  end

  defp apply_single(state, {:compound_delete_prefix, prefix}) do
    redis_key = Ferricstore.Store.CompoundKey.extract_redis_key(prefix)

    case check_key_lock(state, redis_key, nil) do
      :ok -> do_compound_delete_prefix(state, redis_key, prefix)
      {:error, :key_locked} -> {:error, :key_locked}
    end
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

  defp apply_single(state, {:getset, key, new_value}) do
    do_getset(state, key, new_value)
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

  defp apply_single(state, {:json_op, cmd, args}) do
    Ferricstore.Commands.Json.handle(cmd, args, build_sm_store(state))
  end

  defp apply_single(state, {:hll_op, cmd, args}) do
    Ferricstore.Commands.HyperLogLog.handle(cmd, args, build_sm_store(state))
  end

  defp apply_single(state, {:bitmap_op, cmd, args}) do
    Ferricstore.Commands.Bitmap.handle(cmd, args, build_sm_store(state))
  end

  defp apply_single(state, {:geo_op, cmd, args}) do
    Ferricstore.Commands.Geo.handle(cmd, args, build_sm_store(state))
  end

  defp apply_single(state, {:tdigest_op, cmd, args}) do
    Ferricstore.Commands.TDigest.handle(cmd, args, build_sm_store(state))
  end

  defp apply_single(state, {:cas, key, expected, new_value, ttl_ms}) do
    do_cas(state, key, expected, new_value, ttl_ms)
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

  # -- Probabilistic data structure commands in batch/cross_shard_tx --

  defp apply_single(state, {:bloom_create, key, num_bits, num_hashes, prob_meta}) do
    do_prob_command(state, fn ->
      create_bloom_metadata(state, key, num_bits, num_hashes, prob_meta)
    end)
  end

  defp apply_single(state, {:bloom_add, key, element, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "bloom")
      ensure_prob_dir(state)

      case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
        :ok -> NIF.bloom_file_add(path, element)
        {:error, _reason} = error -> error
      end
    end)
  end

  defp apply_single(state, {:bloom_madd, key, elements, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "bloom")
      ensure_prob_dir(state)

      case auto_create_bloom_if_needed(state, path, key, auto_create_params) do
        :ok -> NIF.bloom_file_madd(path, elements)
        {:error, _reason} = error -> error
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

  defp apply_single(state, {:cms_merge, dst_key, src_paths, weights, create_params}) do
    do_prob_command(state, fn ->
      dst_path = prob_path(state, dst_key, "cms")
      ensure_prob_dir(state)

      case maybe_create_cms_merge_dst(state, dst_path, dst_key, create_params) do
        :ok -> NIF.cms_file_merge(dst_path, src_paths, weights)
        {:error, _reason} = error -> error
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
      ensure_prob_dir(state)

      case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
        :ok -> NIF.cuckoo_file_add(path, element)
        {:error, _reason} = error -> error
      end
    end)
  end

  defp apply_single(state, {:cuckoo_addnx, key, element, auto_create_params}) do
    do_prob_command(state, fn ->
      path = prob_path(state, key, "cuckoo")
      ensure_prob_dir(state)

      case auto_create_cuckoo_if_needed(state, path, key, auto_create_params) do
        :ok -> NIF.cuckoo_file_addnx(path, element)
        {:error, _reason} = error -> error
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

  defp apply_origin_async_put(state, key, value, expire_at_ms) do
    expected_value = value_for_ets(value, hot_cache_threshold(state))

    case :ets.lookup(state.ets, key) do
      [{^key, ^expected_value, ^expire_at_ms, _lfu, :pending, 0, _vs}]
      when expected_value != nil ->
        queue_pending_put(key, to_disk_binary(value), expire_at_ms)
        :ok

      [{^key, ^expected_value, ^expire_at_ms, _lfu, fid, off, vs}]
      when fid != :pending and valid_cold_location(fid, off, vs) ->
        :ok

      [{^key, _other_value, _other_exp, _lfu, :pending, _off, _vs}] ->
        :ok

      _ ->
        apply_single(state, {:put, key, value, expire_at_ms})
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
    case do_get_meta(state, key) do
      {^expected_value, ^expire_at_ms} when expected_value != nil ->
        :already_applied

      {^before_value, ^before_expire_at_ms} when before_value != nil ->
        :apply

      nil when before_value == nil ->
        :apply

      _other ->
        pending_newer_origin_replay_decision(state, key, inner_cmd, expected_value)
    end
  end

  defp pending_newer_origin_replay_decision(state, key, inner_cmd, expected_value) do
    case :ets.lookup(state.ets, key) do
      [{^key, current_value, _expire_at_ms, _lfu, :pending, _off, _value_size}] ->
        pending_origin_replay_decision(inner_cmd, current_value, expected_value)

      _ ->
        :newer_local_value
    end
  end

  defp pending_origin_replay_decision(_inner_cmd, expected_value, expected_value) do
    :already_applied
  end

  defp pending_origin_replay_decision(inner_cmd, current_value, expected_value) do
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
    Process.put(:sm_cross_shard_pending_writes, [])
    Process.put(:sm_cross_shard_pending_originals, %{})

    try do
      result = fun.()

      case flush_cross_shard_pending_writes() do
        :ok ->
          result

        {:error, _reason} = error ->
          rollback_cross_shard_pending_writes(state)
          error
      end
    after
      Process.delete(:sm_cross_shard_pending_writes)
      Process.delete(:sm_cross_shard_pending_originals)
    end
  end

  defp flush_cross_shard_pending_writes do
    pending =
      :sm_cross_shard_pending_writes
      |> Process.put([])
      |> Enum.reverse()

    pending
    |> Enum.group_by(&cross_shard_pending_target/1)
    |> Enum.reduce_while(:ok, fn {{file_path, file_id, keydir}, entries}, :ok ->
      batch = Enum.map(entries, &cross_shard_pending_to_batch_entry/1)
      append_result = append_pending_batch(file_path, batch)
      validated_append_result = validate_append_result(batch, append_result)

      case validated_append_result do
        {:ok, locations} ->
          apply_cross_shard_pending_locations(keydir, file_id, entries, locations)
          {:cont, :ok}

        {:error, reason} ->
          {:halt, {:error, {:bitcask_append_failed, reason}}}
      end
    end)
  end

  defp cross_shard_pending_target(
         {:put, _idx, keydir, file_path, file_id, _key, _ets, _disk, _exp}
       ),
       do: {file_path, file_id, keydir}

  defp cross_shard_pending_target({:delete, _idx, keydir, file_path, file_id, _key}),
    do: {file_path, file_id, keydir}

  defp cross_shard_pending_to_batch_entry(
         {:put, _idx, _keydir, _file_path, _file_id, key, _ets_value, disk_value, expire_at_ms}
       ),
       do: {:put, key, disk_value, expire_at_ms}

  defp cross_shard_pending_to_batch_entry({:delete, _idx, _keydir, _file_path, _file_id, key}),
    do: {:delete, key, nil}

  defp apply_cross_shard_pending_locations(keydir, file_id, entries, locations) do
    Enum.zip(entries, locations)
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
    Process.put(:sm_pending_writes, [])
    Process.put(:sm_pending_originals, %{})
    Process.put(:sm_pending_values, %{})
    started_at = System.monotonic_time()

    try do
      result = fun.()
      flush_result = flush_pending_writes(state)
      emit_raft_apply_telemetry(state, started_at, result, flush_result)

      case flush_result do
        :ok -> result
        {:error, _reason} = error -> error
      end
    after
      Process.delete(:sm_pending_writes)
      Process.delete(:sm_pending_originals)
      Process.delete(:sm_pending_values)
    end
  end

  defp do_put(state, key, value, expire_at_ms) do
    maybe_clear_compound_data_structure_for_string_put(state, key)

    ets_val = value_for_ets(value, hot_cache_threshold(state))
    disk_val = to_disk_binary(value)

    # Track binary memory: subtract old entry's bytes, add new entry's bytes.
    # This gives MemoryGuard accurate off-heap binary accounting.
    track_keydir_binary_delta(state, key, ets_val)
    record_pending_original(state, key)

    # Insert into ETS immediately so subsequent read-modify-write commands
    # (INCR, APPEND, etc.) in the same batch see the correct value.
    # The file_id is :pending — flush_pending_writes will update it with
    # the real offset after the batch NIF call.
    :ets.insert(
      state.ets,
      {key, ets_val, expire_at_ms, LFU.initial(), :pending, 0, byte_size(disk_val)}
    )

    # Accumulate for batch disk write — flushed by flush_pending_writes
    # at the end of apply/3 before returning to ra.
    queue_pending_put(key, disk_val, expire_at_ms)

    :ok
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

  # Flushes all accumulated disk writes in a single NIF call, then updates
  # ETS entries with real file_id/offset. Called at the end of every apply/3
  # — no :pending entries remain after this returns.
  defp flush_pending_writes(state) do
    case Process.put(:sm_pending_writes, []) do
      [] ->
        :ok

      pending when is_list(pending) ->
        batch = Enum.reverse(pending)
        batch_bytes = bitcask_batch_bytes(batch)
        record_bytes = bitcask_record_bytes(batch)
        delete_count = Enum.count(batch, &match?({:delete, _, _}, &1))

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
                state = track_bitcask_append_bytes(state, file_path, file_id, record_bytes)
                Process.put(:sm_pending_state, state)
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

  defp bitcask_batch_bytes(batch) do
    Enum.reduce(batch, 0, fn
      {:put, key, value, _expire_at_ms}, acc ->
        acc + byte_size(key) + byte_size(value)

      {:delete, key, _prob_path}, acc ->
        acc + byte_size(key)
    end)
  end

  defp bitcask_record_bytes(batch) do
    Enum.reduce(batch, 0, fn
      {:put, key, value, _expire_at_ms}, acc ->
        acc + @bitcask_record_header_size + byte_size(key) + byte_size(value)

      {:delete, key, _prob_path}, acc ->
        acc + @bitcask_record_header_size + byte_size(key)
    end)
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
    if Enum.any?(batch, &match?({:delete, _, _}, &1)) do
      ops =
        Enum.map(batch, fn
          {:put, key, value, expire_at_ms} -> {:put, key, value, expire_at_ms}
          {:delete, key, _prob_path} -> {:delete, key}
        end)

      NIF.v2_append_ops_batch_nosync(file_path, ops)
    else
      puts =
        Enum.map(batch, fn {:put, key, value, expire_at_ms} ->
          {key, value, expire_at_ms}
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

  defp validate_append_result(batch, {:ok, locations}) do
    case validate_pending_locations(batch, locations) do
      :ok -> {:ok, locations}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_append_result(_batch, append_result), do: append_result

  defp validate_pending_locations(batch, locations) when length(batch) != length(locations) do
    {:error,
     {:bitcask_append_result_mismatch, {:length_mismatch, length(batch), length(locations)}}}
  end

  defp validate_pending_locations(batch, locations) do
    batch
    |> Enum.zip(locations)
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {{entry, location}, index}, :ok ->
      expected = pending_entry_op(entry)
      actual = pending_location_op(location)

      if expected == actual do
        {:cont, :ok}
      else
        {:halt,
         {:error, {:bitcask_append_result_mismatch, {:op_mismatch, index, expected, actual}}}}
      end
    end)
  end

  defp pending_entry_op({:put, _key, _value, _expire_at_ms}), do: :put
  defp pending_entry_op({:delete, _key, _prob_path}), do: :delete

  defp pending_location_op({:put, _offset, _value_size}), do: :put
  defp pending_location_op({:delete, _offset, _record_size}), do: :delete
  defp pending_location_op(_location), do: :unknown

  defp apply_pending_locations(state, file_id, batch, locations) do
    Enum.zip(batch, locations)
    |> Enum.each(fn
      {{:put, key, val, exp}, {:put, offset, value_size}} ->
        apply_put_pending_location(state, key, val, exp, file_id, offset, value_size)

      {{:delete, _key, nil}, {:delete, _offset, _record_size}} ->
        :ok

      {{:delete, _key, prob_path}, {:delete, _offset, _record_size}} ->
        maybe_delete_prob_file_path(state, prob_path)
    end)
  end

  defp apply_put_pending_location(state, key, value, expire_at_ms, file_id, offset, value_size) do
    expected_value = value_for_ets(value, hot_cache_threshold(state))
    expected_staged_size = byte_size(to_disk_binary(value))

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

  defp queue_pending_delete(key, prob_path) do
    pending = Process.get(:sm_pending_writes, [])
    Process.put(:sm_pending_writes, [{:delete, key, prob_path} | pending])
    pending_values = Process.get(:sm_pending_values, %{})
    Process.put(:sm_pending_values, Map.delete(pending_values, key))
  end

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
          :atomics.put(ctx.checkpoint_flags, flag_idx, 1)
        end

        Ferricstore.Store.DiskPressure.clear(ctx, state.shard_index)
    end
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

  defp duration_us(started_at) do
    System.monotonic_time()
    |> Kernel.-(started_at)
    |> System.convert_time_unit(:native, :microsecond)
  end

  defp record_pending_original(state, key) do
    originals = Process.get(:sm_pending_originals, %{})

    if Map.has_key?(originals, key) do
      :ok
    else
      original =
        case :ets.lookup(state.ets, key) do
          [entry] -> {:entry, entry}
          [] -> :missing
        end

      Process.put(:sm_pending_originals, Map.put(originals, key, original))
    end
  end

  defp rollback_pending_writes(state) do
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
    flush_pending_for_key(state, key)
    prob_path = prob_file_path_for_delete(state, key)

    case resolve_active_file(state) do
      :stale ->
        set_disk_pressure(state)
        {:error, :active_file_unavailable}

      {_file_path, _file_id} ->
        record_pending_original(state, key)
        track_keydir_binary_remove(state, key)
        :ets.delete(state.ets, key)
        queue_pending_delete(key, prob_path)
        :ok
    end
  end

  # Flushes the BitcaskWriter if the key has a pending background write.
  # Called before tombstone writes and delete_prefix operations to ensure
  # correct disk ordering (PUT before TOMBSTONE).
  defp flush_pending_for_key(state, key) do
    case :ets.lookup(state.ets, key) do
      [{^key, _v, _e, _lfu, :pending, _off, _vs}] ->
        try do
          BitcaskWriter.flush(state.shard_index)
        rescue
          _ -> :ok
        catch
          :exit, _ -> :ok
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
  defp hot_cache_threshold(%{instance_ctx: ctx}) when ctx != nil, do: ctx.hot_cache_max_value_size
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
        do_delete(state, key)
      end

      old
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
      new_str
    end
  end

  # Builds a single-shard store that satisfies the command-handler store
  # interface (get/put/delete/exists?/keys). Writes always go through
  # `do_put` on the LOCAL shard (the one Raft routed this command to).
  # Reads prefer local ETS but fall back to `Router.get` — reads are
  # side-effect-free and safe from any process, which lets commands like
  # PFMERGE read source keys that hash to other shards.
  #
  # Intended for RMW commands migrated into Raft (JSON, PFADD, BITOP, etc.).
  # All writes go through do_put so they participate in with_pending_writes
  # batching.
  defp build_sm_store(state) do
    local_read = fn key ->
      case do_get(state, key) do
        nil ->
          # Not on this shard — fall back to cross-shard Router.get if we
          # have an instance_ctx. This covers PFMERGE-style multi-source
          # reads. If no ctx (uncommon in production) try the default
          # instance as a fallback so bitop/pfmerge still work in tests
          # that don't thread instance_ctx through to state machine config.
          case instance_ctx_for_state(state) do
            nil -> nil
            c -> Router.get(c, key)
          end

        v ->
          v
      end
    end

    %{
      get: local_read,
      get_meta: fn key ->
        case do_get_meta(state, key) do
          nil -> nil
          {v, exp} -> {v, exp}
        end
      end,
      batch_get: fn keys -> sm_store_batch_get(state, keys) end,
      put: fn key, value, expire_at_ms ->
        do_put(state, key, value, expire_at_ms)
        :ok
      end,
      delete: fn key ->
        do_delete(state, key)
        :ok
      end,
      exists?: fn key ->
        if live_key?(state, key) do
          true
        else
          case instance_ctx_for_state(state) do
            nil -> false
            ctx -> Router.exists?(ctx, key)
          end
        end
      end,
      keys: fn ->
        :ets.select(state.ets, [{{:"$1", :_, :_, :_, :_, :_, :_}, [], [:"$1"]}])
      end,
      # Compound ops: redis_key and compound_key live on the same shard by
      # design, so treat them identically to plain get/put/delete.
      compound_get: fn rk, ck ->
        case sm_store_compound_get(state, rk, ck) do
          nil ->
            case instance_ctx_for_state(state) do
              nil -> nil
              ctx -> Router.compound_get(ctx, rk, ck)
            end

          value ->
            value
        end
      end,
      compound_get_meta: fn rk, ck ->
        case sm_store_compound_get_meta(state, rk, ck) do
          nil -> nil
          {v, exp} -> {v, exp}
        end
      end,
      compound_batch_get: fn rk, compound_keys ->
        sm_store_compound_batch_get(state, rk, compound_keys)
      end,
      compound_batch_get_meta: fn rk, compound_keys ->
        sm_store_compound_batch_get_meta(state, rk, compound_keys)
      end,
      compound_put: fn _rk, ck, value, expire_at_ms ->
        do_put(state, ck, value, expire_at_ms)
        :ok
      end,
      compound_delete: fn _rk, ck ->
        do_delete(state, ck)
        :ok
      end,
      compound_scan: fn rk, prefix ->
        local_keys = sm_store_compound_local_keys(state, prefix)

        if local_keys != [] do
          sm_store_compound_scan(state, rk, prefix, local_keys)
        else
          case instance_ctx_for_state(state) do
            nil ->
              []

            ctx ->
              Router.compound_scan(ctx, rk, prefix)
          end
        end
      end,
      compound_count: fn rk, prefix ->
        local_count =
          state
          |> sm_store_compound_local_keys(prefix)
          |> Enum.count(fn key -> sm_store_live_local_key?(state, key) end)

        if local_count > 0 do
          local_count
        else
          case instance_ctx_for_state(state) do
            nil ->
              0

            ctx ->
              Router.compound_count(ctx, rk, prefix)
          end
        end
      end,
      compound_delete_prefix: fn _rk, prefix ->
        keys =
          :ets.select(
            state.ets,
            [{{:"$1", :_, :_, :_, :_, :_, :_}, [], [:"$1"]}]
          )
          |> Enum.filter(fn k -> String.starts_with?(k, prefix) end)

        Enum.each(keys, fn k -> do_delete(state, k) end)
        :ok
      end
    }
  end

  defp sm_store_compound_local_keys(state, prefix) do
    prefix_size = byte_size(prefix)

    :ets.select(state.ets, [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_size},
           {:==, {:binary_part, :"$1", 0, prefix_size}, prefix}}}
       ], [:"$1"]}
    ])
  end

  defp sm_store_compound_get(state, redis_key, compound_key) do
    case sm_store_compound_path_fun(state, redis_key, compound_key) do
      nil ->
        do_get(state, compound_key)

      path_fun ->
        case sm_store_batch_get(state, [compound_key], path_fun) do
          [value] -> value
          _ -> nil
        end
    end
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

  defp sm_store_compound_scan(state, redis_key, prefix, local_keys) do
    path_fun = sm_store_compound_path_fun(state, redis_key, prefix) || (&sm_file_path/2)
    sm_store_compound_scan(state, local_keys, path_fun)
  end

  defp sm_store_compound_scan(state, local_keys, path_fun) do
    values = sm_store_batch_get(state, local_keys, path_fun)

    local_keys
    |> Enum.zip(values)
    |> Enum.flat_map(fn
      {key, value} when is_binary(value) -> [{sm_prefix_field(key), value}]
      {_key, _value} -> []
    end)
    |> Enum.sort_by(fn {field, _value} -> field end)
  end

  defp sm_store_compound_path_fun(state, redis_key, compound_key_or_prefix) do
    case promoted_compound_path(state, redis_key, compound_key_or_prefix) do
      nil -> nil
      dedicated_path -> fn _state, fid -> sm_file_path_from_path(dedicated_path, fid) end
    end
  end

  defp sm_store_live_local_key?(state, key) do
    now = apply_now_ms()

    case :ets.lookup(state.ets, key) do
      [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
        true

      [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        true

      [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        true

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when exp > now and valid_cold_location(fid, off, vsize) ->
        true

      [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
        track_keydir_binary_remove_known(state, key, value)
        :ets.delete(state.ets, key)
        false

      _ ->
        false
    end
  end

  defp sm_store_batch_get(state, keys), do: sm_store_batch_get(state, keys, &sm_file_path/2)

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

    cold_reads
    |> Enum.zip(values)
    |> Enum.reduce(results, fn
      {{index, key, exp, fid, off}, value}, acc when is_binary(value) ->
        ets_value = value_for_ets(value, hot_cache_threshold(state))
        track_keydir_binary_warm(state, ets_value)
        :ets.insert(state.ets, {key, ets_value, exp, LFU.initial(), fid, off, byte_size(value)})
        Map.put(acc, index, value)

      {_read, _value}, acc ->
        acc
    end)
  end

  defp sm_store_batch_remote_get([], _ctx, results), do: results
  defp sm_store_batch_remote_get(_entries, nil, results), do: results

  defp sm_store_batch_remote_get(entries, ctx, results) do
    remote_keys = Enum.map(entries, fn {_index, key} -> key end)
    remote_values = Router.batch_get(ctx, remote_keys)

    merge_indexed_values(results, entries, remote_values)
  end

  defp sm_store_compound_batch_get(state, redis_key, compound_keys) do
    state
    |> sm_store_compound_batch_get_meta(redis_key, compound_keys)
    |> Enum.map(fn
      {value, _exp} -> value
      nil -> nil
    end)
  end

  defp sm_store_compound_batch_get_meta(_state, _redis_key, []), do: []

  defp sm_store_compound_batch_get_meta(state, redis_key, compound_keys) do
    path_fun =
      sm_store_compound_path_fun(state, redis_key, List.first(compound_keys)) || (&sm_file_path/2)

    {local_results, cold_reads, remote_keys} =
      compound_keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, [], []}, fn {compound_key, index}, {results, cold, remote} ->
        sm_store_collect_compound_batch_get_meta(
          state,
          compound_key,
          index,
          results,
          cold,
          remote
        )
      end)

    local_results =
      sm_store_read_cold_meta_batch(state, local_results, Enum.reverse(cold_reads), path_fun)

    results =
      case {Enum.reverse(remote_keys), instance_ctx_for_state(state)} do
        {[], _ctx} ->
          local_results

        {_remote, nil} ->
          local_results

        {remote, ctx} ->
          remote_compound_keys = Enum.map(remote, fn {_index, compound_key} -> compound_key end)
          remote_values = Router.compound_batch_get_meta(ctx, redis_key, remote_compound_keys)

          remote
          |> Enum.zip(remote_values)
          |> Enum.reduce(local_results, fn
            {{index, _compound_key}, {value, exp}}, acc ->
              Map.put(acc, index, {value, exp})

            {_remote_entry, _missing}, acc ->
              acc
          end)
      end

    compound_keys
    |> Enum.with_index()
    |> Enum.map(fn {_compound_key, index} -> Map.get(results, index) end)
  end

  defp sm_store_collect_compound_batch_get_meta(state, key, index, results, cold, remote) do
    case sm_pending_value_meta(key) do
      {:hit, value, exp} ->
        {Map.put(results, index, {value, exp}), cold, remote}

      :miss ->
        sm_store_collect_committed_batch_get_meta(state, key, index, results, cold, remote)
    end
  end

  defp sm_store_collect_committed_batch_get_meta(state, key, index, results, cold, remote) do
    now = apply_now_ms()

    case :ets.lookup(state.ets, key) do
      [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
        {Map.put(results, index, {value, 0}), cold, remote}

      [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        {results, [{index, key, 0, fid, off} | cold], remote}

      [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        {Map.put(results, index, {value, exp}), cold, remote}

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

  defp sm_store_read_cold_meta_batch(_state, results, [], _path_fun), do: results

  defp sm_store_read_cold_meta_batch(state, results, cold_reads, path_fun) do
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

    cold_reads
    |> Enum.zip(values)
    |> Enum.reduce(results, fn
      {{index, key, exp, fid, off}, value}, acc when is_binary(value) ->
        ets_value = value_for_ets(value, hot_cache_threshold(state))
        track_keydir_binary_warm(state, ets_value)
        :ets.insert(state.ets, {key, ets_value, exp, LFU.initial(), fid, off, byte_size(value)})
        Map.put(acc, index, {value, exp})

      {_read, _value}, acc ->
        acc
    end)
  end

  defp merge_indexed_values(results, entries, values) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce(results, fn {{index, _key}, value}, acc ->
      Map.put(acc, index, value)
    end)
  end

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

      {%{state | cross_shard_locks: new_locks}, :ok}
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

    {%{state | cross_shard_locks: new_locks}, :ok}
  end

  # Checks whether a key is locked by someone other than owner_ref.
  defp check_key_lock(state, key, owner_ref) do
    locks = Map.get(state, :cross_shard_locks, %{})
    now = apply_now_ms()

    case Map.get(locks, key) do
      nil -> :ok
      {^owner_ref, _exp} -> :ok
      {_other, exp} when exp <= now -> :ok
      {_other, _exp} -> {:error, :key_locked}
    end
  end

  # Writes an intent record. Returns {new_state, :ok}.
  defp do_write_intent(state, owner_ref, intent_map) do
    intents = Map.get(state, :cross_shard_intents, %{})
    {%{state | cross_shard_intents: Map.put(intents, owner_ref, intent_map)}, :ok}
  end

  # Deletes an intent record. Returns {new_state, :ok}.
  defp do_delete_intent(state, owner_ref) do
    intents = Map.get(state, :cross_shard_intents, %{})
    {%{state | cross_shard_intents: Map.delete(intents, owner_ref)}, :ok}
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
        warm_from_disk(state, key, 0, fid, off)

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
        warm_from_disk(state, key, exp, fid, off)

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
  defp warm_from_disk(state, key, expire_at_ms, fid, off) do
    path = sm_file_path(state, fid)

    case read_cold_async(path, off, key) do
      {:ok, value} when is_binary(value) ->
        v = value_for_ets(value, hot_cache_threshold(state))
        # Cold -> warm: previous ETS value was nil, only new value bytes matter
        track_keydir_binary_warm(state, v)
        :ets.insert(state.ets, {key, v, expire_at_ms, LFU.initial(), fid, off, byte_size(value)})
        {:hit, value, expire_at_ms}

      _ ->
        :miss
    end
  end

  defp read_cold_async(path, offset, key) do
    Ferricstore.Store.ColdRead.pread_at(path, offset, key, @cold_read_timeout_ms)
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
      compound_delete: fn _redis_key, compound_key ->
        do_delete(state, compound_key)
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
    case promoted_compound_path(state, redis_key, compound_key) do
      nil ->
        do_put(state, compound_key, value, expire_at_ms)

      dedicated_path ->
        do_promoted_compound_put(state, compound_key, value, expire_at_ms, dedicated_path)
    end
  end

  defp do_compound_delete(state, redis_key, compound_key) do
    case promoted_compound_path(state, redis_key, compound_key) do
      nil -> do_delete(state, compound_key)
      dedicated_path -> do_promoted_compound_delete(state, compound_key, dedicated_path)
    end
  end

  defp do_compound_delete_prefix(state, redis_key, prefix) do
    case promoted_compound_path(state, redis_key, prefix) do
      nil ->
        do_delete_prefix(state, prefix)

      _dedicated_path ->
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
  end

  defp do_promoted_compound_put(state, compound_key, value, expire_at_ms, dedicated_path) do
    value_for = value_for_ets(value, hot_cache_threshold(state))
    disk_val = to_disk_binary(value)
    active = Promotion.find_active(dedicated_path)
    fid = parse_fid_from_path(active)

    case NIF.v2_append_record(active, compound_key, disk_val, expire_at_ms) do
      {:ok, {offset, _record_size}} ->
        value_size = byte_size(disk_val)

        track_keydir_binary_delta(state, compound_key, value_for)

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

  defp do_promoted_compound_delete(state, compound_key, dedicated_path) do
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

      case do_get(state, type_key) do
        nil ->
          :ok

        type ->
          clear_compound_prefix_for_string_put(state, key, type)
          do_delete(state, type_key)
      end
    end

    :ok
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
  defp track_keydir_binary_delta(state, key, new_ets_val) do
    ref = keydir_binary_ref(state)

    if ref do
      new_bytes = binary_byte_size(key) + binary_byte_size(new_ets_val)

      old_bytes =
        case :ets.lookup(state.ets, key) do
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

    if ref do
      bytes =
        case :ets.lookup(state.ets, key) do
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

  # Returns the prob directory for this shard.
  defp prob_dir(%{shard_data_path: shard_data_path}) do
    Path.join(shard_data_path, "prob")
  end

  # Ensures the prob directory exists. Fsyncs parent on first create
  # so the new dir's entry survives kernel panic.
  defp ensure_prob_dir(state) do
    dir = prob_dir(state)

    unless Ferricstore.FS.exists?(dir) do
      Ferricstore.FS.mkdir_p!(dir)
      _ = NIF.v2_fsync_dir(Path.dirname(dir))
    end
  end

  # Called immediately after a `*_file_create` NIF to make the new
  # filename entry durable. The NIF already fsynced the file's data;
  # this fsyncs the directory so the entry itself is durable.
  defp prob_fsync_dir(state) do
    NIF.v2_fsync_dir(prob_dir(state))
  end

  defp create_bloom_metadata(state, key, num_bits, num_hashes, prob_meta) do
    path = prob_path(state, key, "bloom")
    ensure_prob_dir(state)

    with :ok <- prob_create_and_fsync(state, NIF.bloom_file_create(path, num_bits, num_hashes)) do
      do_put(state, key, :erlang.term_to_binary(prob_meta), 0)
      :ok
    end
  end

  defp create_cms_metadata(state, key, width, depth) do
    path = prob_path(state, key, "cms")
    ensure_prob_dir(state)

    with :ok <- prob_create_and_fsync(state, NIF.cms_file_create(path, width, depth)) do
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

      with :ok <- prob_create_and_fsync(state, NIF.cms_file_create(dst_path, width, depth)) do
        meta_val = {:cms_meta, %{width: width, depth: depth}}
        do_put(state, dst_key, :erlang.term_to_binary(meta_val), 0)
        :ok
      end
    end
  end

  defp create_cuckoo_metadata(state, key, capacity, bucket_size) do
    path = prob_path(state, key, "cuckoo")
    ensure_prob_dir(state)

    with :ok <- prob_create_and_fsync(state, NIF.cuckoo_file_create(path, capacity, bucket_size)) do
      meta_val = {:cuckoo_meta, %{capacity: capacity}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end
  end

  defp create_topk_metadata(state, key, k, width, depth, decay) do
    path = prob_path(state, key, "topk")
    ensure_prob_dir(state)

    with :ok <-
           prob_create_and_fsync(state, NIF.topk_file_create_v2(path, k, width, depth, decay)) do
      meta_val = {:topk_meta, %{path: path, k: k, width: width, depth: depth, decay: decay}}
      do_put(state, key, :erlang.term_to_binary(meta_val), 0)
      :ok
    end
  end

  defp prob_create_and_fsync(state, create_result) do
    with :ok <- normalize_prob_create_result(create_result),
         :ok <- normalize_prob_create_result(prob_fsync_dir(state)) do
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

        with :ok <- prob_create_and_fsync(state, NIF.bloom_file_create(path, nb, nh)) do
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

        with :ok <- prob_create_and_fsync(state, NIF.cuckoo_file_create(path, cap, bs)) do
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
          case :erlang.binary_to_term(value) do
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
    try do
      if Ferricstore.FS.rm(path) == :ok, do: prob_fsync_dir(state), else: :ok
    rescue
      _ -> :ok
    end
  end
end
