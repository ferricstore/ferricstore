defmodule Ferricstore.Store.Ops do
  @moduledoc """
  Unified interface for store operations.

  Dispatches based on the type of `store` argument:
  - `%FerricStore.Instance{}` struct -> calls Router directly
  - `%LocalTxStore{}` struct -> local ETS for same-shard, Router for remote
  - map (closure-based store) -> calls the closure

  This allows incremental migration from closure maps to instance structs
  without changing command handler logic.
  """

  alias Ferricstore.Store.Router
  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Reads, as: ShardReads
  alias Ferricstore.Store.Shard.Writes, as: ShardWrites

  @typep store :: FerricStore.Instance.t() | LocalTxStore.t() | map()
  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808
  @overflow_error "ERR increment or decrement would overflow"

  # --- Basic key operations ---

  @spec get(store(), binary()) :: binary() | nil
  def get(%FerricStore.Instance{} = ctx, key), do: Router.get(ctx, key)

  def get(%LocalTxStore{} = tx, key) do
    if local?(tx, key) do
      if tx_deleted?(key), do: nil, else: local_read_value(tx, key)
    else
      Router.get(tx.instance_ctx, key)
    end
  end

  def get(store, key) when is_map(store), do: store.get.(key)

  @spec get_meta(store(), binary()) :: {binary(), non_neg_integer()} | nil
  def get_meta(%FerricStore.Instance{} = ctx, key), do: Router.get_meta(ctx, key)

  def get_meta(%LocalTxStore{} = tx, key) do
    if local?(tx, key) do
      if tx_deleted?(key), do: nil, else: local_read_meta(tx, key)
    else
      Router.get_meta(tx.instance_ctx, key)
    end
  end

  def get_meta(store, key) when is_map(store), do: store.get_meta.(key)

  @spec put(store(), binary(), binary(), non_neg_integer()) :: :ok | {:error, binary()}
  def put(%FerricStore.Instance{} = ctx, key, value, exp), do: Router.put(ctx, key, value, exp)

  def put(%LocalTxStore{} = tx, key, value, exp) do
    if local?(tx, key) do
      ShardETS.ets_insert(tx.shard_state, key, value, exp)
      tx_undelete(key)
      send(self(), {:tx_pending_write, key, value, exp})
      :ok
    else
      Router.put(tx.instance_ctx, key, value, exp)
    end
  end

  def put(store, key, value, exp) when is_map(store), do: store.put.(key, value, exp)

  @spec set(store(), binary(), binary(), map()) :: term()
  def set(%FerricStore.Instance{} = ctx, key, value, opts), do: Router.set(ctx, key, value, opts)

  def set(%LocalTxStore{} = tx, key, value, opts) do
    if local?(tx, key) do
      local_set(tx, key, value, opts)
    else
      Router.set(tx.instance_ctx, key, value, opts)
    end
  end

  def set(store, key, value, opts) when is_map(store) do
    case store do
      %{set: set_fun} when is_function(set_fun, 3) ->
        set_fun.(key, value, opts)

      _ ->
        fallback_set(store, key, value, opts)
    end
  end

  @spec delete(store(), binary()) :: :ok
  def delete(%FerricStore.Instance{} = ctx, key), do: Router.delete(ctx, key)

  def delete(%LocalTxStore{} = tx, key) do
    if local?(tx, key) do
      ShardETS.ets_delete_key(tx.shard_state, key)
      tx_mark_deleted(key)
      send(self(), {:tx_pending_delete, key})
      :ok
    else
      Router.delete(tx.instance_ctx, key)
    end
  end

  def delete(store, key) when is_map(store), do: store.delete.(key)

  @spec exists?(store(), binary()) :: boolean()
  def exists?(%FerricStore.Instance{} = ctx, key), do: Router.exists?(ctx, key)

  def exists?(%LocalTxStore{} = tx, key) do
    if local?(tx, key) do
      if tx_deleted?(key) do
        false
      else
        case ShardETS.ets_lookup_warm(tx.shard_state, key) do
          {:hit, _, _} -> true
          :expired -> false
          :miss ->
            case ShardReads.v2_local_read(tx.shard_state, key) do
              {:ok, nil} -> false
              {:ok, _value} -> true
              _error -> false
            end
        end
      end
    else
      Router.exists?(tx.instance_ctx, key)
    end
  end

  def exists?(store, key) when is_map(store), do: store.exists?.(key)

  @spec keys(store()) :: [binary()]
  def keys(%FerricStore.Instance{} = ctx), do: Router.keys(ctx)
  def keys(%LocalTxStore{} = tx), do: Router.keys(tx.instance_ctx)
  def keys(store) when is_map(store), do: store.keys.()

  @spec dbsize(store()) :: non_neg_integer()
  def dbsize(%FerricStore.Instance{} = ctx), do: Router.dbsize(ctx)
  def dbsize(%LocalTxStore{} = tx), do: Router.dbsize(tx.instance_ctx)
  def dbsize(store) when is_map(store), do: store.dbsize.()

  # --- Numeric operations ---

  @spec incr(store(), binary(), integer()) :: {:ok, integer()} | {:error, binary()}
  def incr(%FerricStore.Instance{} = ctx, key, delta), do: Router.incr(ctx, key, delta)

  def incr(%LocalTxStore{} = tx, key, delta) do
    if local?(tx, key) do
      current = local_read_value_for_rmw(tx, key)

      case current do
        nil ->
          case checked_integer_add(0, delta) do
            {:ok, new_val} ->
              ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
              send(self(), {:tx_pending_write, key, new_val, 0})
              {:ok, new_val}

            :overflow ->
              {:error, @overflow_error}
          end

        value ->
          case ShardETS.coerce_integer(value) do
            {:ok, int_val} ->
              case checked_integer_add(int_val, delta) do
                {:ok, new_val} ->
                  ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
                  send(self(), {:tx_pending_write, key, new_val, 0})
                  {:ok, new_val}

                :overflow ->
                  {:error, @overflow_error}
              end

            :error ->
              {:error, "ERR value is not an integer or out of range"}
          end
      end
    else
      Router.incr(tx.instance_ctx, key, delta)
    end
  end

  def incr(store, key, delta) when is_map(store), do: store.incr.(key, delta)

  defp checked_integer_add(value, delta) do
    result = value + delta

    if result > @max_int64 or result < @min_int64 do
      :overflow
    else
      {:ok, result}
    end
  end

  @spec incr_float(store(), binary(), float()) :: {:ok, binary()} | {:error, binary()}
  def incr_float(%FerricStore.Instance{} = ctx, key, delta), do: Router.incr_float(ctx, key, delta)

  def incr_float(%LocalTxStore{} = tx, key, delta) do
    if local?(tx, key) do
      current = local_read_value_for_rmw(tx, key)

      case current do
        nil ->
          new_val = delta * 1.0
          ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
          send(self(), {:tx_pending_write, key, new_val, 0})
          {:ok, new_val}

        value ->
          case ShardETS.coerce_float(value) do
            {:ok, float_val} ->
              new_val = float_val + delta
              ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
              send(self(), {:tx_pending_write, key, new_val, 0})
              {:ok, new_val}

            :error ->
              {:error, "ERR value is not a valid float"}
          end
      end
    else
      Router.incr_float(tx.instance_ctx, key, delta)
    end
  end

  def incr_float(store, key, delta) when is_map(store), do: store.incr_float.(key, delta)

  # --- String mutation operations ---

  @spec append(store(), binary(), binary()) :: {:ok, non_neg_integer()}
  def append(%FerricStore.Instance{} = ctx, key, suffix), do: Router.append(ctx, key, suffix)

  def append(%LocalTxStore{} = tx, key, suffix) do
    if local?(tx, key) do
      current =
        case ShardETS.ets_lookup_warm(tx.shard_state, key) do
          {:hit, value, _exp} -> ShardETS.to_disk_binary(value)
          :expired -> ""
          :miss ->
            case ShardReads.v2_local_read(tx.shard_state, key) do
              {:ok, nil} -> ""
              {:ok, v} -> v
              _ -> ""
            end
        end

      new_val = current <> suffix
      ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
      send(self(), {:tx_pending_write, key, new_val, 0})
      {:ok, byte_size(new_val)}
    else
      Router.append(tx.instance_ctx, key, suffix)
    end
  end

  def append(store, key, suffix) when is_map(store), do: store.append.(key, suffix)

  @spec getset(store(), binary(), binary()) :: binary() | nil
  def getset(%FerricStore.Instance{} = ctx, key, value), do: Router.getset(ctx, key, value)

  def getset(%LocalTxStore{} = tx, key, new_value) do
    if local?(tx, key) do
      old = local_read_value_for_rmw(tx, key)
      ShardETS.ets_insert(tx.shard_state, key, new_value, 0)
      send(self(), {:tx_pending_write, key, new_value, 0})
      old
    else
      Router.getset(tx.instance_ctx, key, new_value)
    end
  end

  def getset(store, key, value) when is_map(store), do: store.getset.(key, value)

  @spec getdel(store(), binary()) :: binary() | nil
  def getdel(%FerricStore.Instance{} = ctx, key), do: Router.getdel(ctx, key)

  def getdel(%LocalTxStore{} = tx, key) do
    if local?(tx, key) do
      old = local_read_value_for_rmw(tx, key)

      if old do
        ShardETS.ets_delete_key(tx.shard_state, key)
        send(self(), {:tx_pending_delete, key})
      end

      old
    else
      Router.getdel(tx.instance_ctx, key)
    end
  end

  def getdel(store, key) when is_map(store), do: store.getdel.(key)

  @spec getex(store(), binary(), non_neg_integer()) :: binary() | nil
  def getex(%FerricStore.Instance{} = ctx, key, exp), do: Router.getex(ctx, key, exp)

  def getex(%LocalTxStore{} = tx, key, expire_at_ms) do
    if local?(tx, key) do
      value = local_read_value_for_rmw(tx, key)

      if value do
        ShardETS.ets_insert(tx.shard_state, key, value, expire_at_ms)
        send(self(), {:tx_pending_write, key, value, expire_at_ms})
      end

      value
    else
      Router.getex(tx.instance_ctx, key, expire_at_ms)
    end
  end

  def getex(store, key, exp) when is_map(store), do: store.getex.(key, exp)

  @spec setrange(store(), binary(), non_neg_integer(), binary()) :: {:ok, non_neg_integer()}
  def setrange(%FerricStore.Instance{} = ctx, key, offset, value), do: Router.setrange(ctx, key, offset, value)

  def setrange(%LocalTxStore{} = tx, key, offset, value) do
    if local?(tx, key) do
      old =
        case ShardETS.ets_lookup_warm(tx.shard_state, key) do
          {:hit, v, _exp} -> ShardETS.to_disk_binary(v)
          :expired -> ""
          :miss ->
            case ShardReads.v2_local_read(tx.shard_state, key) do
              {:ok, nil} -> ""
              {:ok, v} -> v
              _ -> ""
            end
        end

      new_val = ShardWrites.apply_setrange(old, offset, value)
      ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
      send(self(), {:tx_pending_write, key, new_val, 0})
      {:ok, byte_size(new_val)}
    else
      Router.setrange(tx.instance_ctx, key, offset, value)
    end
  end

  def setrange(store, key, offset, value) when is_map(store), do: store.setrange.(key, offset, value)

  # --- Native operations ---

  @spec cas(store(), binary(), binary(), binary(), non_neg_integer() | nil) :: 1 | 0 | nil
  def cas(%FerricStore.Instance{} = ctx, key, expected, new_val, ttl), do: Router.cas(ctx, key, expected, new_val, ttl)
  def cas(%LocalTxStore{} = tx, key, expected, new_val, ttl), do: Router.cas(tx.instance_ctx, key, expected, new_val, ttl)
  def cas(store, key, expected, new_val, ttl) when is_map(store), do: store.cas.(key, expected, new_val, ttl)

  @spec lock(store(), binary(), binary(), pos_integer()) :: :ok | {:error, binary()}
  def lock(%FerricStore.Instance{} = ctx, key, owner, ttl), do: Router.lock(ctx, key, owner, ttl)
  def lock(%LocalTxStore{} = tx, key, owner, ttl), do: Router.lock(tx.instance_ctx, key, owner, ttl)
  def lock(store, key, owner, ttl) when is_map(store), do: store.lock.(key, owner, ttl)

  @spec unlock(store(), binary(), binary()) :: 1 | {:error, binary()}
  def unlock(%FerricStore.Instance{} = ctx, key, owner), do: Router.unlock(ctx, key, owner)
  def unlock(%LocalTxStore{} = tx, key, owner), do: Router.unlock(tx.instance_ctx, key, owner)
  def unlock(store, key, owner) when is_map(store), do: store.unlock.(key, owner)

  @spec extend(store(), binary(), binary(), pos_integer()) :: 1 | {:error, binary()}
  def extend(%FerricStore.Instance{} = ctx, key, owner, ttl), do: Router.extend(ctx, key, owner, ttl)
  def extend(%LocalTxStore{} = tx, key, owner, ttl), do: Router.extend(tx.instance_ctx, key, owner, ttl)
  def extend(store, key, owner, ttl) when is_map(store), do: store.extend.(key, owner, ttl)

  @spec ratelimit_add(store(), binary(), pos_integer(), pos_integer(), pos_integer()) :: [term()]
  def ratelimit_add(%FerricStore.Instance{} = ctx, key, window, max, count), do: Router.ratelimit_add(ctx, key, window, max, count)
  def ratelimit_add(%LocalTxStore{} = tx, key, window, max, count), do: Router.ratelimit_add(tx.instance_ctx, key, window, max, count)
  def ratelimit_add(store, key, window, max, count) when is_map(store), do: store.ratelimit_add.(key, window, max, count)

  # --- List operations ---

  @spec list_op(store(), binary(), term()) :: term()
  def list_op(%FerricStore.Instance{} = ctx, key, op), do: Router.list_op(ctx, key, op)
  def list_op(%LocalTxStore{} = tx, key, op), do: Router.list_op(tx.instance_ctx, key, op)
  def list_op(store, key, op) when is_map(store), do: store.list_op.(key, op)

  # --- Compound key capability check ---

  @spec has_compound?(store()) :: boolean()
  def has_compound?(%FerricStore.Instance{}), do: true
  def has_compound?(%LocalTxStore{}), do: true
  def has_compound?(store) when is_map(store), do: is_map_key(store, :compound_get)

  # --- Compound key operations ---

  @spec compound_get(store(), binary(), binary()) :: binary() | nil
  def compound_get(%FerricStore.Instance{} = ctx, redis_key, compound_key), do: Router.compound_get(ctx, redis_key, compound_key)

  def compound_get(%LocalTxStore{} = tx, redis_key, compound_key) do
    if local?(tx, redis_key) do
      local_read_value(tx, compound_key)
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_get, redis_key, compound_key})
    end
  end

  def compound_get(store, redis_key, compound_key) when is_map(store), do: store.compound_get.(redis_key, compound_key)

  @spec compound_get_meta(store(), binary(), binary()) :: {binary(), non_neg_integer()} | nil
  def compound_get_meta(%FerricStore.Instance{} = ctx, redis_key, compound_key), do: Router.compound_get_meta(ctx, redis_key, compound_key)

  def compound_get_meta(%LocalTxStore{} = tx, redis_key, compound_key) do
    if local?(tx, redis_key) do
      local_read_meta(tx, compound_key)
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
    end
  end

  def compound_get_meta(store, redis_key, compound_key) when is_map(store), do: store.compound_get_meta.(redis_key, compound_key)

  @spec compound_put(store(), binary(), binary(), binary(), non_neg_integer()) :: :ok
  def compound_put(%FerricStore.Instance{} = ctx, redis_key, compound_key, value, exp), do: Router.compound_put(ctx, redis_key, compound_key, value, exp)

  def compound_put(%LocalTxStore{} = tx, redis_key, compound_key, value, expire_at_ms) do
    if local?(tx, redis_key) do
      ShardETS.ets_insert(tx.shard_state, compound_key, value, expire_at_ms)
      send(self(), {:tx_pending_write, compound_key, value, expire_at_ms})
      :ok
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
    end
  end

  def compound_put(store, redis_key, compound_key, value, exp) when is_map(store), do: store.compound_put.(redis_key, compound_key, value, exp)

  @spec compound_delete(store(), binary(), binary()) :: :ok
  def compound_delete(%FerricStore.Instance{} = ctx, redis_key, compound_key), do: Router.compound_delete(ctx, redis_key, compound_key)

  def compound_delete(%LocalTxStore{} = tx, redis_key, compound_key) do
    if local?(tx, redis_key) do
      ShardETS.ets_delete_key(tx.shard_state, compound_key)
      send(self(), {:tx_pending_delete, compound_key})
      :ok
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_delete, redis_key, compound_key})
    end
  end

  def compound_delete(store, redis_key, compound_key) when is_map(store), do: store.compound_delete.(redis_key, compound_key)

  @spec compound_scan(store(), binary(), binary()) :: [{binary(), binary()}]
  def compound_scan(%FerricStore.Instance{} = ctx, redis_key, prefix), do: Router.compound_scan(ctx, redis_key, prefix)

  def compound_scan(%LocalTxStore{} = tx, redis_key, prefix) do
    if local?(tx, redis_key) do
      results = ShardETS.prefix_scan_entries(tx.shard_state.keydir, prefix, tx.shard_state.shard_data_path)
      Enum.sort_by(results, fn {field, _} -> field end)
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_scan, redis_key, prefix})
    end
  end

  def compound_scan(store, redis_key, prefix) when is_map(store), do: store.compound_scan.(redis_key, prefix)

  @spec compound_count(store(), binary(), binary()) :: non_neg_integer()
  def compound_count(%FerricStore.Instance{} = ctx, redis_key, prefix), do: Router.compound_count(ctx, redis_key, prefix)

  def compound_count(%LocalTxStore{} = tx, redis_key, prefix) do
    if local?(tx, redis_key) do
      ShardETS.prefix_count_entries(tx.shard_state.keydir, prefix)
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_count, redis_key, prefix})
    end
  end

  def compound_count(store, redis_key, prefix) when is_map(store), do: store.compound_count.(redis_key, prefix)

  @spec compound_delete_prefix(store(), binary(), binary()) :: :ok
  def compound_delete_prefix(%FerricStore.Instance{} = ctx, redis_key, prefix), do: Router.compound_delete_prefix(ctx, redis_key, prefix)

  def compound_delete_prefix(%LocalTxStore{} = tx, redis_key, prefix) do
    if local?(tx, redis_key) do
      keys_to_delete = ShardETS.prefix_collect_keys(tx.shard_state.keydir, prefix)

      Enum.each(keys_to_delete, fn key ->
        ShardETS.ets_delete_key(tx.shard_state, key)
        send(self(), {:tx_pending_delete, key})
      end)

      :ok
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
    end
  end

  def compound_delete_prefix(store, redis_key, prefix) when is_map(store), do: store.compound_delete_prefix.(redis_key, prefix)

  # --- Prob operations ---

  @spec prob_write(store(), tuple()) :: term()
  def prob_write(%FerricStore.Instance{} = ctx, command), do: Router.prob_write(ctx, command)
  def prob_write(%LocalTxStore{} = tx, command), do: Router.prob_write(tx.instance_ctx, command)
  def prob_write(store, command) when is_map(store), do: store.prob_write.(command)

  @spec prob_dir(store(), binary()) :: binary()
  def prob_dir(%FerricStore.Instance{} = ctx, key) do
    idx = Router.shard_for(ctx, key)
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
    Path.join(shard_path, "prob")
  end

  def prob_dir(%LocalTxStore{} = tx, _key) do
    Path.join(tx.shard_state.shard_data_path, "prob")
  end

  def prob_dir(store, _key) when is_map(store) and is_map_key(store, :prob_dir), do: store.prob_dir.()
  def prob_dir(store, key) when is_map(store) and is_map_key(store, :prob_dir_for_key), do: store.prob_dir_for_key.(key)

  # --- Flush ---

  @spec flush(store()) :: :ok
  def flush(%FerricStore.Instance{} = ctx) do
    Enum.each(Router.keys(ctx), fn k -> Router.delete(ctx, k) end)
    :ok
  end

  def flush(%LocalTxStore{} = tx) do
    Enum.each(Router.keys(tx.instance_ctx), fn k -> Router.delete(tx.instance_ctx, k) end)
    :ok
  end

  def flush(store) when is_map(store), do: store.flush.()

  # --- On push callback (for Waiters notification) ---

  @spec on_push(store(), binary()) :: :ok | nil
  def on_push(store, key) do
    case store do
      %FerricStore.Instance{} -> Ferricstore.Waiters.notify_push(key)
      %LocalTxStore{} -> Ferricstore.Waiters.notify_push(key)
      store when is_map(store) -> if fun = store[:on_push], do: fun.(key)
    end
  end

  # ===================================================================
  # Private helpers for LocalTxStore
  # ===================================================================

  defp local?(tx, key), do: Router.shard_for(tx.instance_ctx, key) == tx.shard_index

  defp tx_deleted?(key) do
    deleted = Process.get(:tx_deleted_keys, MapSet.new())
    MapSet.member?(deleted, key)
  end

  defp tx_mark_deleted(key) do
    deleted = Process.get(:tx_deleted_keys, MapSet.new())
    Process.put(:tx_deleted_keys, MapSet.put(deleted, key))
  end

  defp tx_undelete(key) do
    deleted = Process.get(:tx_deleted_keys, MapSet.new())

    if MapSet.member?(deleted, key) do
      Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
    end
  end

  # Read value from local ETS, cold-read fallback. Returns value or nil.
  defp local_read_value(tx, key) do
    case ShardETS.ets_lookup_warm(tx.shard_state, key) do
      {:hit, value, _exp} -> value
      :expired -> nil
      :miss ->
        case ShardReads.v2_local_read(tx.shard_state, key) do
          {:ok, nil} -> nil
          {:ok, value} ->
            ShardETS.ets_insert(tx.shard_state, key, value, 0)
            value
          _error -> nil
        end
    end
  end

  # Read {value, expire_at_ms} from local ETS, cold-read fallback. Returns {value, exp} or nil.
  defp local_read_meta(tx, key) do
    case ShardETS.ets_lookup_warm(tx.shard_state, key) do
      {:hit, value, exp} -> {value, exp}
      :expired -> nil
      :miss ->
        case ShardReads.v2_local_read(tx.shard_state, key) do
          {:ok, nil} -> nil
          {:ok, value} ->
            ShardETS.ets_insert(tx.shard_state, key, value, 0)
            {value, 0}
          _error -> nil
        end
    end
  end

  defp local_set(tx, key, value, opts) do
    {old_value, effective_expire} =
      if opts.get or opts.keepttl do
        case local_read_meta(tx, key) do
          nil ->
            {nil, opts.expire_at_ms}

          {old_val, old_exp} ->
            {old_val, if(opts.keepttl, do: old_exp, else: opts.expire_at_ms)}
        end
      else
        {nil, opts.expire_at_ms}
      end

    skip? =
      cond do
        opts.nx and local_read_meta(tx, key) != nil -> true
        opts.xx and local_read_meta(tx, key) == nil -> true
        true -> false
      end

    if skip? do
      if opts.get, do: old_value, else: nil
    else
      ShardETS.ets_insert(tx.shard_state, key, value, effective_expire)
      tx_undelete(key)
      send(self(), {:tx_pending_write, key, value, effective_expire})
      if opts.get, do: old_value, else: :ok
    end
  end

  defp fallback_set(store, key, value, opts) do
    {old_value, effective_expire} =
      if opts.get or opts.keepttl do
        case get_meta(store, key) do
          nil ->
            {nil, opts.expire_at_ms}

          {old_val, old_exp} ->
            {old_val, if(opts.keepttl, do: old_exp, else: opts.expire_at_ms)}
        end
      else
        {nil, opts.expire_at_ms}
      end

    skip? =
      cond do
        opts.nx and exists?(store, key) -> true
        opts.xx and not exists?(store, key) -> true
        true -> false
      end

    if skip? do
      if opts.get, do: old_value, else: nil
    else
      put(store, key, value, effective_expire)
      if opts.get, do: old_value, else: :ok
    end
  end

  # Read value for read-modify-write ops (incr, getset, getdel, getex).
  # Same as local_read_value but without warming ETS on cold read (matching original closures).
  defp local_read_value_for_rmw(tx, key) do
    case ShardETS.ets_lookup_warm(tx.shard_state, key) do
      {:hit, value, _exp} -> value
      :expired -> nil
      :miss ->
        case ShardReads.v2_local_read(tx.shard_state, key) do
          {:ok, nil} -> nil
          {:ok, v} -> v
          _ -> nil
        end
    end
  end
end
