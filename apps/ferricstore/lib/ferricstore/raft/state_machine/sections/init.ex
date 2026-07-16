defmodule Ferricstore.Raft.StateMachine.Sections.Init do
  @moduledoc false

  import Kernel, except: [apply: 3]

  defmacro __using__(_opts) do
    quote do
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
      alias Ferricstore.Store.Shard.LogicalKeyIndex
      alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
      alias Ferricstore.Store.Shard.Flush, as: ShardFlush
      alias Ferricstore.Transaction.Ast, as: TxAst

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

        instance_ctx = Map.get(config, :instance_ctx)

        apply_context =
          case Map.get(config, :apply_context) do
            %Ferricstore.Raft.ApplyContext{} = context ->
              context

            _missing ->
              case instance_ctx do
                %{apply_context: %Ferricstore.Raft.ApplyContext{} = context} ->
                  context

                _missing ->
                  Ferricstore.Raft.ApplyContext.from_runtime()
              end
          end

        {default_logical_key_index, default_logical_key_slots} =
          LogicalKeyIndex.table_names(
            Map.get(config, :instance_name, :default),
            config.shard_index
          )

        logical_key_index_name =
          Map.get(config, :logical_key_index_name, default_logical_key_index)

        logical_key_slots_name =
          Map.get(config, :logical_key_slots_name, default_logical_key_slots)

        LogicalKeyIndex.ensure_tables!(logical_key_index_name, logical_key_slots_name)

        %{
          shard_index: config.shard_index,
          shard_data_path: config.shard_data_path,
          shard_data_path_expanded: Path.expand(config.shard_data_path),
          active_file_id: config.active_file_id,
          active_file_path: config.active_file_path,
          ets: config.ets,
          data_dir: data_dir,
          data_dir_expanded: Path.expand(data_dir),
          instance_ctx: instance_ctx,
          promoted_instances: Map.get(config, :promoted_instances, %{}),
          apply_context: apply_context,
          apply_context_encoded: Ferricstore.Raft.ApplyContext.encode(apply_context),
          instance_name: Map.get(config, :instance_name, :default),
          blob_side_channel_threshold_bytes:
            Map.get(config, :blob_side_channel_threshold_bytes, BlobValue.threshold(instance_ctx)),
          zset_score_index_name:
            Map.get(config, :zset_score_index_name) ||
              elem(
                ZSetIndex.table_names(
                  Map.get(config, :instance_name, :default),
                  config.shard_index
                ),
                0
              ),
          zset_score_lookup_name:
            Map.get(config, :zset_score_lookup_name) ||
              elem(
                ZSetIndex.table_names(
                  Map.get(config, :instance_name, :default),
                  config.shard_index
                ),
                1
              ),
          compound_member_index_name:
            Map.get(config, :compound_member_index_name) ||
              Ferricstore.Store.Shard.CompoundMemberIndex.table_name(
                Map.get(config, :instance_name, :default),
                config.shard_index
              ),
          compound_revision_index_name: compound_revision_index_name(config),
          logical_key_index_name: logical_key_index_name,
          logical_key_slots_name: logical_key_slots_name,
          flow_index_name:
            Map.get(config, :flow_index_name) ||
              elem(
                NativeFlowIndex.table_names(
                  Map.get(config, :instance_name, :default),
                  config.shard_index
                ),
                0
              ),
          flow_lookup_name:
            Map.get(config, :flow_lookup_name) ||
              elem(
                NativeFlowIndex.table_names(
                  Map.get(config, :instance_name, :default),
                  config.shard_index
                ),
                1
              ),
          flow_lmdb_path:
            Map.get_lazy(config, :flow_lmdb_path, fn ->
              Ferricstore.Flow.LMDB.path(config.shard_data_path)
            end),
          flow_lmdb_mirror?: false,
          flow_due_catalog: Ferricstore.Flow.DueCatalog.new(),
          flow_hibernation_promotion_cursor: nil,
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
          cross_shard_lock_expiries: :gb_trees.empty(),
          cross_shard_intents: %{}
        }
        |> ensure_flow_native_index_registered()
      end

      defp compound_revision_index_name(config) do
        table =
          Map.get(config, :compound_revision_index_name) ||
            Ferricstore.Store.Shard.CompoundRevisionIndex.table_name(
              Map.get(config, :instance_name, :default),
              config.shard_index
            )

        Ferricstore.Store.Shard.CompoundRevisionIndex.ensure_table!(table)
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
    end
  end
end
