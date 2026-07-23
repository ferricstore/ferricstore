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
  @invalid_footprint_error {:error, "ERR invalid cross-shard key footprint"}

  @too_many_keys_error "ERR cross-shard operation exceeds max key limit (#{@max_cross_shard_keys}). " <>
                         "Use hash tags {tag} to colocate keys on the same shard."

  @typedoc "Role for a key in a multi-key operation."
  @type key_role :: :read | :write | :read_write

  @typedoc "Key with its role in the operation."
  @type key_with_role :: {binary(), key_role()}

  @read_callbacks [
    :get,
    :get_meta,
    :exists?,
    :compound_get,
    :compound_get_meta,
    :compound_batch_get,
    :compound_batch_get_meta,
    :compound_scan,
    :compound_scan_slice,
    :compound_count,
    :zset_score_range,
    :zset_score_range_slice,
    :zset_score_count,
    :zset_rank_range,
    :zset_member_rank,
    :prob_dir_for
  ]

  @write_callbacks [
    :put,
    :delete,
    :incr,
    :incr_float,
    :append,
    :getset,
    :getdel,
    :getex,
    :setrange,
    :cas,
    :lock,
    :unlock,
    :extend,
    :ratelimit_add,
    :defer_stream_cleanup,
    :compound_put,
    :compound_batch_put,
    :compound_delete,
    :compound_batch_delete,
    :compound_delete_prefix
  ]

  @doc """
  Executes a multi-key operation in the caller's storage context.

  Durable contexts reject keys that span independent Raft groups. Non-Raft
  contexts retain the local journaled coordinator.
  """
  @spec execute([key_with_role()], (map() -> term()), keyword()) :: term()
  def execute(keys_with_roles, execute_fn, opts \\ [])

  def execute([], _execute_fn, _opts),
    do: {:error, "ERR cross-shard operation requires at least one key"}

  def execute(keys_with_roles, execute_fn, opts) when is_list(keys_with_roles) do
    caller_store = Keyword.get(opts, :store)

    if direct_store?(caller_store) do
      if valid_key_footprint?(keys_with_roles) do
        execute_fn.(caller_store)
      else
        @invalid_footprint_error
      end
    else
      execute_with_instance(keys_with_roles, execute_fn, opts, caller_store)
    end
  end

  def execute(_invalid_keys_with_roles, _execute_fn, _opts), do: @invalid_footprint_error

  defp execute_with_instance(keys_with_roles, execute_fn, opts, caller_store) do
    ctx =
      Keyword.get(opts, :instance) ||
        if match?(%FerricStore.Instance{}, caller_store) do
          caller_store
        else
          FerricStore.Instance.get(:default)
        end

    case group_keys_by_shard(ctx, keys_with_roles) do
      {:ok, shard_map} ->
        execute_routed(ctx, shard_map, keys_with_roles, execute_fn, caller_store)

      :error ->
        @invalid_footprint_error
    end
  end

  defp execute_routed(ctx, shard_map, keys_with_roles, execute_fn, caller_store) do
    if map_size(shard_map) == 1 do
      execute_same_shard(ctx, shard_map, execute_fn, caller_store)
    else
      cond do
        Router.durable_context?(ctx) ->
          @crossslot_error

        length(keys_with_roles) > @max_cross_shard_keys ->
          {:error, @too_many_keys_error}

        true ->
          execute_standalone_cross_shard(ctx, shard_map, keys_with_roles, execute_fn)
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

  defp execute_standalone_cross_shard(ctx, shard_map, keys_with_roles, execute_fn) do
    shard_indices = shard_map |> Map.keys() |> Enum.sort()
    [coordinator | participant_indices] = shard_indices
    execute_fn = guard_execute_fn(keys_with_roles, execute_fn)

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
      compound_scan_slice: fn redis_key, prefix, start, count, total ->
        Router.compound_scan_slice(ctx, redis_key, prefix, start, count, total)
      end,
      compound_count: fn redis_key, prefix -> Router.compound_count(ctx, redis_key, prefix) end,
      compound_delete_prefix: fn redis_key, prefix ->
        Router.compound_delete_prefix(ctx, redis_key, prefix)
      end,
      key_lifecycle: fn command -> Router.key_lifecycle(ctx, command) end
    }
  end

  defp guard_execute_fn(keys_with_roles, execute_fn) do
    footprint = access_footprint(keys_with_roles)
    fn store -> execute_fn.(guard_store(store, footprint)) end
  end

  defp access_footprint(keys_with_roles) do
    Enum.reduce(keys_with_roles, %{read: MapSet.new(), write: MapSet.new()}, fn
      {key, :read}, footprint ->
        %{footprint | read: MapSet.put(footprint.read, key)}

      {key, role}, footprint when role in [:write, :read_write] ->
        %{
          read: MapSet.put(footprint.read, key),
          write: MapSet.put(footprint.write, key)
        }
    end)
  end

  defp guard_store(store, footprint) do
    store
    |> guard_callbacks(@read_callbacks, :read, footprint)
    |> guard_callbacks(@write_callbacks, :write, footprint)
    |> guard_batch_callback(:batch_get, :read, footprint)
    |> guard_batch_callback(:batch_put, :write, footprint)
    |> guard_lifecycle_callback(:key_lifecycle, footprint)
    |> guard_lifecycle_callback(:prob_lifecycle, footprint)
  end

  defp guard_callbacks(store, callback_names, access, footprint) do
    Enum.reduce(callback_names, store, fn callback_name, guarded ->
      case Map.fetch(guarded, callback_name) do
        {:ok, callback} when is_function(callback) ->
          Map.put(
            guarded,
            callback_name,
            guard_first_key_callback(callback, access, footprint)
          )

        _missing ->
          guarded
      end
    end)
  end

  defp guard_first_key_callback(callback, access, footprint) do
    case Function.info(callback, :arity) do
      {:arity, 1} ->
        fn key ->
          authorize_key!(footprint, access, key)
          callback.(key)
        end

      {:arity, 2} ->
        fn key, arg ->
          authorize_key!(footprint, access, key)
          callback.(key, arg)
        end

      {:arity, 3} ->
        fn key, arg1, arg2 ->
          authorize_key!(footprint, access, key)
          callback.(key, arg1, arg2)
        end

      {:arity, 4} ->
        fn key, arg1, arg2, arg3 ->
          authorize_key!(footprint, access, key)
          callback.(key, arg1, arg2, arg3)
        end

      {:arity, 5} ->
        fn key, arg1, arg2, arg3, arg4 ->
          authorize_key!(footprint, access, key)
          callback.(key, arg1, arg2, arg3, arg4)
        end

      {:arity, 6} ->
        fn key, arg1, arg2, arg3, arg4, arg5 ->
          authorize_key!(footprint, access, key)
          callback.(key, arg1, arg2, arg3, arg4, arg5)
        end
    end
  end

  defp guard_batch_callback(store, callback_name, access, footprint) do
    case Map.fetch(store, callback_name) do
      {:ok, callback} when is_function(callback, 1) ->
        Map.put(store, callback_name, fn entries ->
          Enum.each(entries, fn entry ->
            key = if is_tuple(entry), do: elem(entry, 0), else: entry
            authorize_key!(footprint, access, key)
          end)

          callback.(entries)
        end)

      _missing ->
        store
    end
  end

  defp guard_lifecycle_callback(store, callback_name, footprint) do
    case Map.fetch(store, callback_name) do
      {:ok, callback} when is_function(callback, 1) ->
        Map.put(store, callback_name, fn command ->
          authorize_lifecycle!(footprint, command)
          callback.(command)
        end)

      _missing ->
        store
    end
  end

  defp authorize_lifecycle!(footprint, {:copy, source, destination, _replace?}) do
    authorize_key!(footprint, :read, source)
    authorize_key!(footprint, :write, destination)
  end

  defp authorize_lifecycle!(footprint, {command, source, destination})
       when command in [:rename, :renamenx] do
    authorize_key!(footprint, :write, source)
    authorize_key!(footprint, :write, destination)
  end

  defp authorize_lifecycle!(_footprint, command) do
    throw({:transaction_store_failure, {:invalid_cross_shard_lifecycle, command}})
  end

  defp authorize_key!(footprint, access, key) do
    allowed = Map.fetch!(footprint, access)

    unless is_binary(key) and MapSet.member?(allowed, key) do
      throw({:transaction_store_failure, {:cross_shard_footprint_violation, access, key}})
    end

    :ok
  end

  defp group_keys_by_shard(ctx, keys_with_roles) do
    Enum.reduce_while(keys_with_roles, {:ok, %{}}, fn entry, {:ok, shard_map} ->
      if valid_key_with_role?(entry) do
        {key, _role} = entry
        shard = Router.shard_for(ctx, key)
        {:cont, {:ok, Map.update(shard_map, shard, [entry], &[entry | &1])}}
      else
        {:halt, :error}
      end
    end)
  end

  defp valid_key_footprint?(keys_with_roles),
    do: Enum.all?(keys_with_roles, &valid_key_with_role?/1)

  defp valid_key_with_role?({key, role})
       when is_binary(key) and role in [:read, :write, :read_write],
       do: true

  defp valid_key_with_role?(_invalid), do: false
end
