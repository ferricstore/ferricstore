defmodule Ferricstore.CrossShardOp do
  @moduledoc """
  Mini-percolator for cross-shard multi-key operations.

  Provides atomic execution of commands that span multiple shards using
  per-key locking through Raft consensus. Only involved keys are locked;
  the rest of each shard operates normally.

  ## Modes

    * **Same-shard** -- all keys hash to the same shard. The execute function
      is called directly with zero overhead (no locking, no intent).
    * **Quorum cross-shard** -- keys span multiple shards. The protocol is:
      lock (ordered by shard index) -> write intent -> execute -> delete intent -> unlock.

  ## Usage

      CrossShardOp.execute(
        [{source_key, :read_write}, {dest_key, :write}],
        fn store -> ...command logic using unified store... end,
        intent: %{command: :smove, keys: %{source: src, dest: dst}}
      )

  The `execute_fn` receives a unified store map that routes operations to the
  correct shard based on key. The store has the same interface as command
  handlers use, so existing command logic works unchanged.
  """

  alias Ferricstore.HLC
  alias Ferricstore.Raft.BlobCommand
  alias Ferricstore.Raft.Cluster
  alias Ferricstore.Raft.CommandClock
  alias Ferricstore.Store.Router

  require Logger

  @lock_ttl_ms 5_000
  @max_retries 3
  @max_cross_shard_keys 20

  @too_many_keys_error "ERR cross-shard operation exceeds max key limit (#{@max_cross_shard_keys}). " <>
                         "Use hash tags {tag} to colocate keys on the same shard."

  @typedoc "Role for a key in a cross-shard operation."
  @type key_role :: :read | :write | :read_write

  @typedoc "Key with its role in the operation."
  @type key_with_role :: {binary(), key_role()}

  @doc """
  Executes a multi-key operation, handling same-shard and cross-shard cases.

  ## Parameters

    * `keys_with_roles` -- list of `{key, role}` tuples. Role is `:read`,
      `:write`, or `:read_write`.
    * `execute_fn` -- function receiving a unified store map and executing
      the actual command logic. The store routes operations to the correct
      shard based on key. Must return the command result.
    * `opts` -- keyword options:
      * `:intent` -- intent map for crash recovery (required for cross-shard)
      * `:namespace` -- namespace prefix (optional, defaults to extracting
        from first key)

  ## Returns

  The result of `execute_fn`, or `{:error, "CROSSSLOT ..."}` when the command
  cannot be represented safely as a cross-shard transaction.
  """
  @spec execute([key_with_role()], (map() -> term()), keyword()) :: term()
  def execute(keys_with_roles, execute_fn, opts \\ []) do
    caller_store = Keyword.get(opts, :store)

    # Direct stores already know how to execute every operation locally. Check
    # this before touching the default instance so embedded/test callers can use
    # command handlers without starting the Raft-backed application.
    if direct_store?(caller_store) do
      execute_fn.(caller_store)
    else
      execute_with_instance(keys_with_roles, execute_fn, opts, caller_store)
    end
  end

  defp execute_with_instance(keys_with_roles, execute_fn, opts, caller_store) do
    ctx =
      Keyword.get(opts, :instance) ||
        if match?(%FerricStore.Instance{}, caller_store) do
          caller_store
        else
          FerricStore.Instance.get(:default)
        end

    keys = Enum.map(keys_with_roles, fn {key, _role} -> key end)
    shard_map = group_keys_by_shard(ctx, keys_with_roles)

    if map_size(shard_map) == 1 do
      # Same-shard fast path: zero overhead.
      # Use the caller's shard-local store if provided, otherwise build one.
      execute_same_shard(ctx, shard_map, execute_fn, caller_store)
    else
      cond do
        length(keys) > @max_cross_shard_keys ->
          {:error, @too_many_keys_error}

        ctx.name != :default ->
          execute_direct_cross_shard(ctx, shard_map, execute_fn)

        true ->
          execute_cross_shard(ctx, keys_with_roles, shard_map, execute_fn, opts)
      end
    end
  end

  defp direct_store?(caller_store) do
    # Fully-capable map stores (mock stores and pre-built routing stores) should
    # bypass Raft context discovery. Shard-local stores still need instance
    # routing for cross-shard operations.
    is_map(caller_store) and not is_map_key(caller_store, :shard_idx) and
      is_map_key(caller_store, :get)
  end

  # ---------------------------------------------------------------------------
  # Same-shard fast path
  # ---------------------------------------------------------------------------

  defp execute_same_shard(ctx, shard_map, execute_fn, caller_store) do
    # Use the caller's store if it is a shard-local store (has :shard_idx)
    # or a fully-capable store (has :get). Otherwise build a fresh one.
    if is_map(caller_store) and
         (is_map_key(caller_store, :shard_idx) or is_map_key(caller_store, :get)) do
      execute_fn.(caller_store)
    else
      [{shard_idx, _keys}] = Map.to_list(shard_map)
      store = build_store_for_shard(ctx, shard_idx)
      execute_fn.(store)
    end
  end

  # ---------------------------------------------------------------------------
  # Cross-shard quorum path: lock -> intent -> execute -> unlock
  # ---------------------------------------------------------------------------

  defp execute_direct_cross_shard(ctx, shard_map, execute_fn) do
    # Non-Raft instances have no lock or intent machinery. Preserve their
    # direct-shard semantics while still routing each key through the caller
    # instance instead of falling into the default Raft cluster.
    per_shard_stores =
      shard_map
      |> Map.keys()
      |> Map.new(fn idx -> {idx, build_store_for_shard(ctx, idx)} end)

    execute_fn.(build_routing_store(ctx, per_shard_stores))
  end

  defp execute_cross_shard(ctx, keys_with_roles, shard_map, execute_fn, opts) do
    owner_ref = make_ref()

    # Sort shards by index for deadlock prevention
    sorted_shards = shard_map |> Map.keys() |> Enum.sort()

    # Determine which keys need locking (only :write and :read_write roles)
    lock_map = build_lock_map(ctx, keys_with_roles)

    case lock_phase(sorted_shards, lock_map, owner_ref, 0) do
      :ok ->
        # Intent phase: write to coordinator shard (lowest index)
        coordinator_shard = hd(sorted_shards)
        intent_map = Keyword.get(opts, :intent, %{})

        # Build read-only stores to compute value hashes before writing intent
        per_shard_stores =
          Map.new(sorted_shards, fn idx -> {idx, build_store_for_shard(ctx, idx)} end)

        value_hashes = compute_value_hashes(ctx, keys_with_roles, per_shard_stores)
        full_intent = Map.put(intent_map, :value_hashes, value_hashes)

        try do
          case write_intent(coordinator_shard, owner_ref, full_intent) do
            {:ok, :ok, _leader} ->
              # Execute phase: build a unified routing store that uses locked
              # write variants with owner_ref, so writes pass through the lock
              # check in the state machine.
              unified_store = build_locked_routing_store(ctx, per_shard_stores, owner_ref)

              result = execute_fn.(unified_store)

              # Clean up: delete intent, unlock
              delete_intent(coordinator_shard, owner_ref)
              unlock_all(sorted_shards, lock_map, owner_ref)

              result

            {:ok, {:error, reason}, _leader} ->
              unlock_all(sorted_shards, lock_map, owner_ref)
              cross_shard_intent_error(reason)

            {:error, reason} ->
              unlock_all(sorted_shards, lock_map, owner_ref)
              cross_shard_intent_error(reason)

            other ->
              unlock_all(sorted_shards, lock_map, owner_ref)
              cross_shard_intent_error(other)
          end
        rescue
          e ->
            unlock_all(sorted_shards, lock_map, owner_ref)
            reraise e, __STACKTRACE__
        end

      {:error, :keys_locked} ->
        {:error, "ERR cross-shard operation failed: keys are locked by another operation"}
    end
  end

  # ---------------------------------------------------------------------------
  # Lock phase: acquire locks in shard order with retries
  # ---------------------------------------------------------------------------

  defp lock_phase(sorted_shards, lock_map, owner_ref, retry) do
    now = HLC.now_ms()
    expire_at = now + @lock_ttl_ms

    result =
      Enum.reduce_while(sorted_shards, {:ok, []}, fn shard_idx, {:ok, locked} ->
        keys_to_lock = Map.get(lock_map, shard_idx, [])

        if keys_to_lock == [] do
          {:cont, {:ok, locked}}
        else
          shard_id = Cluster.shard_server_id(shard_idx)

          case unwrap_ra_reply(
                 CommandClock.process_command(
                   shard_id,
                   {:lock_keys, keys_to_lock, owner_ref, expire_at}
                 )
               ) do
            {:ok, :ok, _} ->
              {:cont, {:ok, [shard_idx | locked]}}

            {:ok, {:error, :keys_locked}, _} ->
              {:halt, {:error, :keys_locked, locked}}

            {:error, reason} ->
              {:halt, {:error, reason, locked}}
          end
        end
      end)

    case result do
      {:ok, _locked} ->
        :ok

      {:error, :keys_locked, locked_so_far} ->
        # Unlock what we acquired
        unlock_acquired(locked_so_far, lock_map, owner_ref)

        if retry < @max_retries do
          # Exponential backoff: 50ms, 100ms, 200ms
          backoff = (50 * :math.pow(2, retry)) |> round()
          Process.sleep(backoff)
          lock_phase(sorted_shards, lock_map, owner_ref, retry + 1)
        else
          {:error, :keys_locked}
        end

      {:error, reason, locked_so_far} ->
        unlock_acquired(locked_so_far, lock_map, owner_ref)
        {:error, reason}
    end
  end

  # ---------------------------------------------------------------------------
  # Unlock helpers
  # ---------------------------------------------------------------------------

  # Unlock all shards in parallel — no ordering needed for release.
  defp unlock_all(sorted_shards, lock_map, owner_ref) do
    parallel_unlock(sorted_shards, lock_map, owner_ref)
  end

  defp unlock_acquired(locked_shards, lock_map, owner_ref) do
    parallel_unlock(locked_shards, lock_map, owner_ref)
  end

  defp parallel_unlock(shards, lock_map, owner_ref) do
    to_unlock =
      shards
      |> Enum.filter(fn idx -> Map.get(lock_map, idx, []) != [] end)
      |> Enum.map(fn idx -> {idx, Map.get(lock_map, idx, [])} end)

    # Fire-and-forget retry task — caller doesn't wait for retries.
    # First attempt is inline (fast path), retries are async.
    failed = attempt_unlock(to_unlock, owner_ref)

    if failed != [] do
      # Retry failed unlocks in a background task with backoff.
      # Stops after lock TTL expires (no point retrying expired locks).
      Task.start(fn -> retry_unlock(failed, owner_ref, @lock_ttl_ms) end)
    end
  end

  defp attempt_unlock(shards_keys, owner_ref) do
    tasks =
      Enum.map(shards_keys, fn {shard_idx, keys} ->
        {shard_idx, keys,
         Task.async(fn ->
           shard_id = Cluster.shard_server_id(shard_idx)

           unwrap_ra_reply(
             CommandClock.process_command(shard_id, {:unlock_keys, keys, owner_ref})
           )
         end)}
      end)

    results = Task.await_many(Enum.map(tasks, fn {_, _, task} -> task end), 5_000)

    Enum.zip(tasks, results)
    |> Enum.filter(fn {{_idx, _keys, _task}, result} ->
      case result do
        {:ok, :ok, _} -> false
        _ -> true
      end
    end)
    |> Enum.map(fn {{idx, keys, _task}, _result} -> {idx, keys} end)
  end

  defp retry_unlock([], _owner_ref, _remaining_ms), do: :ok
  defp retry_unlock(_failed, _owner_ref, remaining_ms) when remaining_ms <= 0, do: :ok

  defp retry_unlock(failed, owner_ref, remaining_ms) do
    Process.sleep(500)

    still_failed = attempt_unlock(failed, owner_ref)

    if still_failed != [] do
      Logger.warning(
        "CrossShardOp: unlock retry failed on shards #{inspect(Enum.map(still_failed, &elem(&1, 0)))} — " <>
          "#{remaining_ms - 500}ms until TTL expiry"
      )

      retry_unlock(still_failed, owner_ref, remaining_ms - 500)
    end
  end

  # ---------------------------------------------------------------------------
  # Intent helpers
  # ---------------------------------------------------------------------------

  defp write_intent(coordinator_shard, owner_ref, intent_map) do
    full_intent =
      Map.merge(
        %{status: :executing, created_at: HLC.now_ms()},
        intent_map
      )

    shard_id = Cluster.shard_server_id(coordinator_shard)

    unwrap_ra_reply(
      CommandClock.process_command(shard_id, {:cross_shard_intent, owner_ref, full_intent})
    )
  end

  defp delete_intent(coordinator_shard, owner_ref) do
    shard_id = Cluster.shard_server_id(coordinator_shard)
    unwrap_ra_reply(CommandClock.process_command(shard_id, {:delete_intent, owner_ref}))
  end

  defp cross_shard_intent_error(reason) do
    Logger.warning("CrossShardOp: intent write failed: #{inspect(reason)}")
    {:error, "ERR cross-shard operation failed: intent write failed"}
  end

  # ---------------------------------------------------------------------------
  # Store building
  # ---------------------------------------------------------------------------

  @doc false
  @spec build_store_for_shard(non_neg_integer()) :: map()
  def build_store_for_shard(shard_idx) do
    build_store_for_shard(FerricStore.Instance.get(:default), shard_idx)
  end

  @doc false
  @spec build_store_for_shard(FerricStore.Instance.t(), non_neg_integer()) :: map()
  def build_store_for_shard(ctx, shard_idx) do
    # Reads use Router's direct keydir path instead of the Shard GenServer.
    # WARaft applies default-instance writes through its storage backend, so the
    # old shard process is not the source of truth in replacement mode. This
    # also avoids a GenServer hop for same-shard generic and compound commands.
    #
    # Writes still route through Router so they get the selected replication
    # backend and not_leader/forward handling.
    %{
      shard_idx: shard_idx,
      get: fn key -> Router.get(ctx, key) end,
      get_meta: fn key -> Router.get_meta(ctx, key) end,
      put: fn key, value, expire_at_ms -> Router.put(ctx, key, value, expire_at_ms) end,
      delete: fn key -> Router.delete(ctx, key) end,
      exists?: fn key -> Router.exists?(ctx, key) end,
      keys: fn -> Router.keys(ctx) end,
      compound_get: fn redis_key, compound_key ->
        Router.compound_get(ctx, redis_key, compound_key)
      end,
      compound_get_meta: fn redis_key, compound_key ->
        Router.compound_get_meta(ctx, redis_key, compound_key)
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        Router.compound_batch_get(ctx, redis_key, compound_keys)
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        Router.compound_batch_get_meta(ctx, redis_key, compound_keys)
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        Router.compound_put(ctx, redis_key, compound_key, value, expire_at_ms)
      end,
      compound_batch_put: fn redis_key, entries ->
        Router.compound_batch_put(ctx, redis_key, entries)
      end,
      compound_delete: fn redis_key, compound_key ->
        Router.compound_delete(ctx, redis_key, compound_key)
      end,
      compound_batch_delete: fn redis_key, compound_keys ->
        Router.compound_batch_delete(ctx, redis_key, compound_keys)
      end,
      compound_scan: fn redis_key, prefix -> Router.compound_scan(ctx, redis_key, prefix) end,
      compound_count: fn redis_key, prefix -> Router.compound_count(ctx, redis_key, prefix) end,
      compound_delete_prefix: fn redis_key, prefix ->
        Router.compound_delete_prefix(ctx, redis_key, prefix)
      end
    }
  end

  # Builds a unified store that routes operations to the correct shard's store
  # based on the key being operated on. The redis_key in compound operations
  # is used for routing; for plain get/put/delete the key itself routes.
  @doc false
  @spec build_routing_store(map()) :: map()
  def build_routing_store(per_shard_stores) do
    build_routing_store(FerricStore.Instance.get(:default), per_shard_stores)
  end

  @doc false
  @spec build_routing_store(FerricStore.Instance.t(), map()) :: map()
  def build_routing_store(ctx, per_shard_stores) do
    route = fn key ->
      idx = Router.shard_for(ctx, key)
      Map.get(per_shard_stores, idx) || Map.get(per_shard_stores, hd(Map.keys(per_shard_stores)))
    end

    %{
      get: fn key -> route.(key).get.(key) end,
      get_meta: fn key -> route.(key).get_meta.(key) end,
      put: fn key, value, exp -> route.(key).put.(key, value, exp) end,
      delete: fn key -> route.(key).delete.(key) end,
      exists?: fn key -> route.(key).exists?.(key) end,
      keys: fn ->
        Enum.flat_map(per_shard_stores, fn {_idx, store} -> store.keys.() end)
      end,
      compound_get: fn redis_key, ck -> route.(redis_key).compound_get.(redis_key, ck) end,
      compound_get_meta: fn redis_key, ck ->
        route.(redis_key).compound_get_meta.(redis_key, ck)
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        route.(redis_key).compound_batch_get.(redis_key, compound_keys)
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        route.(redis_key).compound_batch_get_meta.(redis_key, compound_keys)
      end,
      compound_put: fn redis_key, ck, v, exp ->
        route.(redis_key).compound_put.(redis_key, ck, v, exp)
      end,
      compound_batch_put: fn redis_key, entries ->
        route.(redis_key).compound_batch_put.(redis_key, entries)
      end,
      compound_delete: fn redis_key, ck -> route.(redis_key).compound_delete.(redis_key, ck) end,
      compound_batch_delete: fn redis_key, compound_keys ->
        route.(redis_key).compound_batch_delete.(redis_key, compound_keys)
      end,
      compound_scan: fn redis_key, prefix ->
        route.(redis_key).compound_scan.(redis_key, prefix)
      end,
      compound_count: fn redis_key, prefix ->
        route.(redis_key).compound_count.(redis_key, prefix)
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        route.(redis_key).compound_delete_prefix.(redis_key, prefix)
      end
    }
  end

  # Builds a unified store for cross-shard operations that uses locked write
  # variants (locked_put, locked_delete, locked_delete_prefix) going through
  # Raft directly with the owner_ref. Read operations use the regular per-shard
  # stores (reads are not blocked by locks).
  @doc false
  @spec build_locked_routing_store(map(), reference()) :: map()
  def build_locked_routing_store(per_shard_stores, owner_ref) do
    build_locked_routing_store(FerricStore.Instance.get(:default), per_shard_stores, owner_ref)
  end

  @doc false
  @spec build_locked_routing_store(FerricStore.Instance.t(), map(), reference()) :: map()
  def build_locked_routing_store(ctx, per_shard_stores, owner_ref) do
    route = fn key ->
      idx = Router.shard_for(ctx, key)
      Map.get(per_shard_stores, idx) || Map.get(per_shard_stores, hd(Map.keys(per_shard_stores)))
    end

    locked_compound_put = fn redis_key, compound_key, value, expire_at_ms ->
      shard_idx = Router.shard_for(ctx, redis_key)
      command = {:locked_put, compound_key, value, expire_at_ms, owner_ref}
      submit_locked_write_command(ctx, shard_idx, command)
    end

    locked_compound_delete = fn redis_key, compound_key ->
      shard_idx = Router.shard_for(ctx, redis_key)
      shard_id = Cluster.shard_server_id(shard_idx)

      case unwrap_ra_reply(
             CommandClock.process_command(shard_id, {:locked_delete, compound_key, owner_ref})
           ) do
        {:ok, result, _} -> result
        {:error, reason} -> {:error, reason}
      end
    end

    %{
      # Reads: use the regular per-shard stores (reads pass through locks)
      get: fn key -> route.(key).get.(key) end,
      get_meta: fn key -> route.(key).get_meta.(key) end,
      exists?: fn key -> route.(key).exists?.(key) end,
      keys: fn ->
        Enum.flat_map(per_shard_stores, fn {_idx, store} -> store.keys.() end)
      end,
      compound_get: fn redis_key, ck -> route.(redis_key).compound_get.(redis_key, ck) end,
      compound_get_meta: fn redis_key, ck ->
        route.(redis_key).compound_get_meta.(redis_key, ck)
      end,
      compound_batch_get: fn redis_key, compound_keys ->
        route.(redis_key).compound_batch_get.(redis_key, compound_keys)
      end,
      compound_batch_get_meta: fn redis_key, compound_keys ->
        route.(redis_key).compound_batch_get_meta.(redis_key, compound_keys)
      end,
      compound_scan: fn redis_key, prefix ->
        route.(redis_key).compound_scan.(redis_key, prefix)
      end,
      compound_count: fn redis_key, prefix ->
        route.(redis_key).compound_count.(redis_key, prefix)
      end,

      # Writes: use locked variants through Raft with owner_ref
      put: fn key, value, expire_at_ms ->
        shard_idx = Router.shard_for(ctx, key)
        command = {:locked_put, key, value, expire_at_ms, owner_ref}
        submit_locked_write_command(ctx, shard_idx, command)
      end,
      delete: fn key ->
        shard_idx = Router.shard_for(ctx, key)
        shard_id = Cluster.shard_server_id(shard_idx)

        case unwrap_ra_reply(
               CommandClock.process_command(shard_id, {:locked_delete, key, owner_ref})
             ) do
          {:ok, result, _} -> result
          {:error, reason} -> {:error, reason}
        end
      end,
      compound_put: fn redis_key, compound_key, value, expire_at_ms ->
        locked_compound_put.(redis_key, compound_key, value, expire_at_ms)
      end,
      compound_batch_put: fn redis_key, entries ->
        Enum.reduce_while(entries, :ok, fn {compound_key, value, expire_at_ms}, :ok ->
          case locked_compound_put.(redis_key, compound_key, value, expire_at_ms) do
            :ok -> {:cont, :ok}
            {:error, _} = err -> {:halt, err}
            other -> {:halt, {:error, other}}
          end
        end)
      end,
      compound_delete: fn redis_key, compound_key ->
        locked_compound_delete.(redis_key, compound_key)
      end,
      compound_batch_delete: fn redis_key, compound_keys ->
        Enum.reduce_while(compound_keys, :ok, fn compound_key, :ok ->
          case locked_compound_delete.(redis_key, compound_key) do
            :ok -> {:cont, :ok}
            {:error, _} = err -> {:halt, err}
            other -> {:halt, {:error, other}}
          end
        end)
      end,
      compound_delete_prefix: fn redis_key, prefix ->
        shard_idx = Router.shard_for(ctx, redis_key)
        shard_id = Cluster.shard_server_id(shard_idx)

        case unwrap_ra_reply(
               CommandClock.process_command(shard_id, {:locked_delete_prefix, prefix, owner_ref})
             ) do
          {:ok, result, _} -> result
          {:error, reason} -> {:error, reason}
        end
      end
    }
  end

  # Computes watch tokens for all keys involved in a cross-shard operation.
  # Cold keys use keydir metadata so large values are not materialized only to
  # write the crash-recovery intent.
  @doc false
  @spec compute_value_hashes([key_with_role()], map()) :: map()
  def compute_value_hashes(keys_with_roles, per_shard_stores) do
    compute_value_hashes(FerricStore.Instance.get(:default), keys_with_roles, per_shard_stores)
  end

  @doc false
  @spec compute_value_hashes(FerricStore.Instance.t(), [key_with_role()], map()) :: map()
  def compute_value_hashes(ctx, keys_with_roles, _per_shard_stores) do
    Map.new(keys_with_roles, fn {key, _role} ->
      {key, Router.watch_token(ctx, key)}
    end)
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp submit_locked_write_command(ctx, shard_idx, command) do
    with {:ok, prepared_command} <- prepare_locked_write_command(ctx, shard_idx, command) do
      shard_id = Cluster.shard_server_id(shard_idx)

      case unwrap_ra_reply(CommandClock.process_command(shard_id, prepared_command)) do
        {:ok, result, _} -> result
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp prepare_locked_write_command(ctx, shard_idx, command) do
    if is_map(ctx) and BlobCommand.side_channel_candidate?(ctx, command) do
      BlobCommand.prepare(ctx, shard_idx, command,
        single_member?: single_member_raft_group?(shard_idx)
      )
    else
      {:ok, command}
    end
  end

  defp single_member_raft_group?(shard_index) do
    case Cluster.members(shard_index, 0) do
      {:ok, members, _leader} when is_list(members) -> length(members) == 1
      _other -> false
    end
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  # Groups keys by shard index, preserving roles.
  defp group_keys_by_shard(ctx, keys_with_roles) do
    Enum.group_by(
      keys_with_roles,
      fn {key, _role} -> Router.shard_for(ctx, key) end,
      fn {key, role} -> {key, role} end
    )
  end

  # Builds a map of shard_index => [keys_to_lock]. Only :write and :read_write
  # roles need locking.
  defp build_lock_map(ctx, keys_with_roles) do
    keys_with_roles
    |> Enum.filter(fn {_key, role} -> role in [:write, :read_write] end)
    |> Enum.group_by(
      fn {key, _role} -> Router.shard_for(ctx, key) end,
      fn {key, _role} -> key end
    )
  end

  # The ferricstore state machine wraps every reply as `{:applied_at, ra_index, real}`
  # so the Batcher can gate on local-apply for read-your-write. CrossShardOp uses
  # Direct command submission surfaces that wrap to the caller, so unwrap before
  # pattern-matching against `{:ok, :ok, _}` etc.
  defp unwrap_ra_reply({:ok, {:applied_at, _idx, real}, leader}), do: {:ok, real, leader}
  defp unwrap_ra_reply(other), do: other
end
