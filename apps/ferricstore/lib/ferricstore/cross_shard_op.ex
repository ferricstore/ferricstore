defmodule Ferricstore.CrossShardOp do
  @moduledoc """
  Execution boundary for multi-key operations that may span storage shards.

  Same-shard commands use a direct store with no coordination overhead.
  Standalone instances coordinate multiple local shards under barriers and use
  the standalone compensation journal. Durable commands spanning independent
  Raft groups fail with `CROSSSLOT`; FerricStore does not expose a cross-group
  mutation protocol without crash-safe commit, rollback, and read snapshots.
  """

  alias Ferricstore.Store.Router

  @max_cross_shard_keys 20
  @crossslot_error {:error, "CROSSSLOT Keys in request don't hash to the same slot"}

  @too_many_keys_error "ERR cross-shard operation exceeds max key limit (#{@max_cross_shard_keys}). " <>
                         "Use hash tags {tag} to colocate keys on the same shard."

  @typedoc "Role for a key in a multi-key operation."
  @type key_role :: :read | :write | :read_write

  @typedoc "Key with its role in the operation."
  @type key_with_role :: {binary(), key_role()}

  @doc """
  Executes a multi-key operation in the caller's storage context.

  Durable contexts reject keys that span independent Raft groups. Non-Raft
  contexts retain the local journaled coordinator.
  """
  @spec execute([key_with_role()], (map() -> term()), keyword()) :: term()
  def execute(keys_with_roles, execute_fn, opts \\ [])

  def execute([], _execute_fn, _opts),
    do: {:error, "ERR cross-shard operation requires at least one key"}

  def execute(keys_with_roles, execute_fn, opts) do
    caller_store = Keyword.get(opts, :store)

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

    shard_map = group_keys_by_shard(ctx, keys_with_roles)

    if map_size(shard_map) == 1 do
      execute_same_shard(ctx, shard_map, execute_fn, caller_store)
    else
      cond do
        Router.durable_context?(ctx) ->
          @crossslot_error

        length(keys_with_roles) > @max_cross_shard_keys ->
          {:error, @too_many_keys_error}

        true ->
          execute_standalone_cross_shard(ctx, shard_map, execute_fn)
      end
    end
  end

  defp direct_store?(caller_store) do
    is_map(caller_store) and not is_map_key(caller_store, :shard_idx) and
      is_map_key(caller_store, :get)
  end

  defp execute_same_shard(ctx, shard_map, execute_fn, caller_store) do
    if is_map(caller_store) and
         (is_map_key(caller_store, :shard_idx) or is_map_key(caller_store, :get)) do
      execute_fn.(caller_store)
    else
      [{shard_idx, _keys}] = Map.to_list(shard_map)
      execute_fn.(build_store_for_shard(ctx, shard_idx))
    end
  end

  defp execute_standalone_cross_shard(ctx, shard_map, execute_fn) do
    shard_indices = shard_map |> Map.keys() |> Enum.sort()
    [coordinator | participant_indices] = shard_indices

    try do
      ctx
      |> Router.shard_name(coordinator)
      |> GenServer.call(
        {:standalone_cross_shard_execute, participant_indices, execute_fn},
        :infinity
      )
    catch
      :exit, reason -> {:error, {:standalone_cross_shard_failed, reason}}
    end
  end

  defp build_store_for_shard(ctx, shard_idx) do
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

  defp group_keys_by_shard(ctx, keys_with_roles) do
    Enum.group_by(
      keys_with_roles,
      fn {key, _role} -> Router.shard_for(ctx, key) end,
      fn {key, role} -> {key, role} end
    )
  end
end
