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

  alias Ferricstore.HLC
  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Reads, as: ShardReads
  alias Ferricstore.Store.Shard.Writes, as: ShardWrites

  @typep store :: FerricStore.Instance.t() | LocalTxStore.t() | map()
  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808
  @overflow_error "ERR increment or decrement would overflow"
  @cold_read_timeout_ms 10_000

  defguardp valid_cold_location(file_id, offset, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
                   is_integer(value_size) and value_size >= 0

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

  @spec batch_get(store(), [binary()]) :: [binary() | nil]
  def batch_get(%FerricStore.Instance{} = ctx, keys), do: Router.batch_get(ctx, keys)

  def batch_get(%LocalTxStore{} = tx, keys), do: local_batch_get(tx, keys)

  def batch_get(store, keys) when is_map(store) do
    case store do
      %{batch_get: batch_get_fun} when is_function(batch_get_fun, 1) ->
        batch_get_fun.(keys)

      _ ->
        Enum.map(keys, &get(store, &1))
    end
  end

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

  @spec expire_at_ms(store(), binary()) :: non_neg_integer() | nil
  def expire_at_ms(%FerricStore.Instance{} = ctx, key), do: Router.expire_at_ms(ctx, key)

  def expire_at_ms(%LocalTxStore{} = tx, key) do
    if local?(tx, key) do
      case tx_pending_meta(key) do
        {_value, exp} ->
          exp

        nil ->
          case ShardETS.ets_lookup(tx.shard_state, key) do
            {:hit, _value, exp} -> exp
            {:cold, _fid, _off, _vsize, exp} -> exp
            _ -> nil
          end
      end
    else
      Router.expire_at_ms(tx.instance_ctx, key)
    end
  end

  def expire_at_ms(store, key) when is_map(store) do
    case store do
      %{expire_at_ms: expire_at_ms} when is_function(expire_at_ms, 1) ->
        expire_at_ms.(key)

      _ ->
        case get_meta(store, key) do
          nil -> nil
          {_value, expire_at_ms} -> expire_at_ms
        end
    end
  end

  @spec value_size(store(), binary()) :: non_neg_integer() | nil
  def value_size(%FerricStore.Instance{} = ctx, key), do: Router.value_size(ctx, key)

  def value_size(%LocalTxStore{} = tx, key) do
    cond do
      not local?(tx, key) ->
        Router.value_size(tx.instance_ctx, key)

      tx_deleted?(key) ->
        nil

      true ->
        case tx_pending_meta(key) do
          {value, _exp} ->
            stored_value_size(value)

          nil ->
            case ShardETS.ets_lookup(tx.shard_state, key) do
              {:hit, value, _exp} -> stored_value_size(value)
              {:cold, _fid, _off, vsize, _exp} -> vsize
              _ -> nil
            end
        end
    end
  end

  def value_size(store, key) when is_map(store) do
    case store do
      %{value_size: value_size} when is_function(value_size, 1) ->
        value_size.(key)

      _ ->
        case get(store, key) do
          nil -> nil
          value -> stored_value_size(value)
        end
    end
  end

  @spec put(store(), binary(), binary(), non_neg_integer()) :: :ok | {:error, binary()}
  def put(%FerricStore.Instance{} = ctx, key, value, exp), do: Router.put(ctx, key, value, exp)

  def put(%LocalTxStore{} = tx, key, value, exp) do
    if local?(tx, key) do
      ShardETS.ets_insert(tx.shard_state, key, value, exp)
      tx_put_pending(key, value, exp)
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
      tx_drop_pending(key)
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
          {:hit, _, _} ->
            true

          :expired ->
            false

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
              tx_put_pending(key, new_val, 0)
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
                  tx_put_pending(key, new_val, 0)
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
  def incr_float(%FerricStore.Instance{} = ctx, key, delta),
    do: Router.incr_float(ctx, key, delta)

  def incr_float(%LocalTxStore{} = tx, key, delta) do
    if local?(tx, key) do
      current = local_read_value_for_rmw(tx, key)

      case current do
        nil ->
          new_val = delta * 1.0
          ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
          tx_put_pending(key, new_val, 0)
          send(self(), {:tx_pending_write, key, new_val, 0})
          {:ok, new_val}

        value ->
          case ShardETS.coerce_float(value) do
            {:ok, float_val} ->
              new_val = float_val + delta
              ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
              tx_put_pending(key, new_val, 0)
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
        case local_read_value_for_rmw(tx, key) do
          nil -> ""
          value -> ShardETS.to_disk_binary(value)
        end

      new_val = current <> suffix
      ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
      tx_put_pending(key, new_val, 0)
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
      tx_put_pending(key, new_value, 0)
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
        tx_drop_pending(key)
        tx_mark_deleted(key)
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
        tx_put_pending(key, value, expire_at_ms)
        send(self(), {:tx_pending_write, key, value, expire_at_ms})
      end

      value
    else
      Router.getex(tx.instance_ctx, key, expire_at_ms)
    end
  end

  def getex(store, key, exp) when is_map(store), do: store.getex.(key, exp)

  @spec setrange(store(), binary(), non_neg_integer(), binary()) :: {:ok, non_neg_integer()}
  def setrange(%FerricStore.Instance{} = ctx, key, offset, value),
    do: Router.setrange(ctx, key, offset, value)

  def setrange(%LocalTxStore{} = tx, key, offset, value) do
    if local?(tx, key) do
      old =
        case local_read_value_for_rmw(tx, key) do
          nil -> ""
          v -> ShardETS.to_disk_binary(v)
        end

      new_val = ShardWrites.apply_setrange(old, offset, value)
      ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
      tx_put_pending(key, new_val, 0)
      send(self(), {:tx_pending_write, key, new_val, 0})
      {:ok, byte_size(new_val)}
    else
      Router.setrange(tx.instance_ctx, key, offset, value)
    end
  end

  def setrange(store, key, offset, value) when is_map(store),
    do: store.setrange.(key, offset, value)

  # --- Native operations ---

  @spec cas(store(), binary(), binary(), binary(), non_neg_integer() | nil) :: 1 | 0 | nil
  def cas(%FerricStore.Instance{} = ctx, key, expected, new_val, ttl),
    do: Router.cas(ctx, key, expected, new_val, ttl)

  def cas(%LocalTxStore{} = tx, key, expected, new_val, ttl),
    do: Router.cas(tx.instance_ctx, key, expected, new_val, ttl)

  def cas(store, key, expected, new_val, ttl) when is_map(store),
    do: store.cas.(key, expected, new_val, ttl)

  @spec lock(store(), binary(), binary(), pos_integer()) :: :ok | {:error, binary()}
  def lock(%FerricStore.Instance{} = ctx, key, owner, ttl), do: Router.lock(ctx, key, owner, ttl)

  def lock(%LocalTxStore{} = tx, key, owner, ttl),
    do: Router.lock(tx.instance_ctx, key, owner, ttl)

  def lock(store, key, owner, ttl) when is_map(store), do: store.lock.(key, owner, ttl)

  @spec unlock(store(), binary(), binary()) :: 1 | {:error, binary()}
  def unlock(%FerricStore.Instance{} = ctx, key, owner), do: Router.unlock(ctx, key, owner)
  def unlock(%LocalTxStore{} = tx, key, owner), do: Router.unlock(tx.instance_ctx, key, owner)
  def unlock(store, key, owner) when is_map(store), do: store.unlock.(key, owner)

  @spec extend(store(), binary(), binary(), pos_integer()) :: 1 | {:error, binary()}
  def extend(%FerricStore.Instance{} = ctx, key, owner, ttl),
    do: Router.extend(ctx, key, owner, ttl)

  def extend(%LocalTxStore{} = tx, key, owner, ttl),
    do: Router.extend(tx.instance_ctx, key, owner, ttl)

  def extend(store, key, owner, ttl) when is_map(store), do: store.extend.(key, owner, ttl)

  @spec ratelimit_add(store(), binary(), pos_integer(), pos_integer(), pos_integer()) :: [term()]
  def ratelimit_add(%FerricStore.Instance{} = ctx, key, window, max, count),
    do: Router.ratelimit_add(ctx, key, window, max, count)

  def ratelimit_add(%LocalTxStore{} = tx, key, window, max, count),
    do: Router.ratelimit_add(tx.instance_ctx, key, window, max, count)

  def ratelimit_add(store, key, window, max, count) when is_map(store),
    do: store.ratelimit_add.(key, window, max, count)

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
  def compound_get(%FerricStore.Instance{} = ctx, redis_key, compound_key),
    do: Router.compound_get(ctx, redis_key, compound_key)

  def compound_get(%LocalTxStore{} = tx, redis_key, compound_key) do
    if local?(tx, redis_key) do
      case promoted_path(tx, redis_key) do
        nil -> local_read_value(tx, compound_key)
        dedicated_path -> local_promoted_read_value(tx, compound_key, dedicated_path)
      end
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_get, redis_key, compound_key})
    end
  end

  def compound_get(store, redis_key, compound_key) when is_map(store),
    do: store.compound_get.(redis_key, compound_key)

  @spec compound_batch_get(store(), binary(), [binary()]) :: [binary() | nil]
  def compound_batch_get(%FerricStore.Instance{} = ctx, redis_key, compound_keys),
    do: Router.compound_batch_get(ctx, redis_key, compound_keys)

  def compound_batch_get(%LocalTxStore{} = tx, redis_key, compound_keys) do
    if local?(tx, redis_key) do
      case promoted_path(tx, redis_key) do
        nil ->
          local_batch_read_values(tx, compound_keys, tx.shard_state.shard_data_path)

        dedicated_path ->
          local_batch_read_values(tx, compound_keys, dedicated_path)
      end
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_batch_get, redis_key, compound_keys})
    end
  end

  def compound_batch_get(store, redis_key, compound_keys) when is_map(store) do
    case store do
      %{compound_batch_get: compound_batch_get_fun} when is_function(compound_batch_get_fun, 2) ->
        compound_batch_get_fun.(redis_key, compound_keys)

      _ ->
        Enum.map(compound_keys, &compound_get(store, redis_key, &1))
    end
  end

  @spec compound_get_meta(store(), binary(), binary()) :: {binary(), non_neg_integer()} | nil
  def compound_get_meta(%FerricStore.Instance{} = ctx, redis_key, compound_key),
    do: Router.compound_get_meta(ctx, redis_key, compound_key)

  def compound_get_meta(%LocalTxStore{} = tx, redis_key, compound_key) do
    if local?(tx, redis_key) do
      case promoted_path(tx, redis_key) do
        nil -> local_read_meta(tx, compound_key)
        dedicated_path -> local_promoted_read_meta(tx, compound_key, dedicated_path)
      end
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_get_meta, redis_key, compound_key})
    end
  end

  def compound_get_meta(store, redis_key, compound_key) when is_map(store),
    do: store.compound_get_meta.(redis_key, compound_key)

  @spec compound_batch_get_meta(store(), binary(), [binary()]) ::
          [{binary(), non_neg_integer()} | nil]
  def compound_batch_get_meta(%FerricStore.Instance{} = ctx, redis_key, compound_keys),
    do: Router.compound_batch_get_meta(ctx, redis_key, compound_keys)

  def compound_batch_get_meta(%LocalTxStore{} = tx, redis_key, compound_keys) do
    if local?(tx, redis_key) do
      case promoted_path(tx, redis_key) do
        nil ->
          local_batch_read_meta(tx, compound_keys, tx.shard_state.shard_data_path)

        dedicated_path ->
          local_batch_read_meta(tx, compound_keys, dedicated_path)
      end
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_batch_get_meta, redis_key, compound_keys})
    end
  end

  def compound_batch_get_meta(store, redis_key, compound_keys) when is_map(store) do
    case store do
      %{compound_batch_get_meta: compound_batch_get_meta_fun}
      when is_function(compound_batch_get_meta_fun, 2) ->
        compound_batch_get_meta_fun.(redis_key, compound_keys)

      _ ->
        Enum.map(compound_keys, &compound_get_meta(store, redis_key, &1))
    end
  end

  @spec compound_put(store(), binary(), binary(), binary(), non_neg_integer()) :: :ok
  def compound_put(%FerricStore.Instance{} = ctx, redis_key, compound_key, value, exp),
    do: Router.compound_put(ctx, redis_key, compound_key, value, exp)

  def compound_put(%LocalTxStore{} = tx, redis_key, compound_key, value, expire_at_ms) do
    if local?(tx, redis_key) do
      ShardETS.ets_insert(tx.shard_state, compound_key, value, expire_at_ms)
      tx_put_pending(compound_key, value, expire_at_ms)
      tx_undelete(compound_key)
      send(self(), {:tx_pending_write, compound_key, value, expire_at_ms})
      :ok
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_put, redis_key, compound_key, value, expire_at_ms})
    end
  end

  def compound_put(store, redis_key, compound_key, value, exp) when is_map(store),
    do: store.compound_put.(redis_key, compound_key, value, exp)

  @spec compound_delete(store(), binary(), binary()) :: :ok
  def compound_delete(%FerricStore.Instance{} = ctx, redis_key, compound_key),
    do: Router.compound_delete(ctx, redis_key, compound_key)

  def compound_delete(%LocalTxStore{} = tx, redis_key, compound_key) do
    if local?(tx, redis_key) do
      ShardETS.ets_delete_key(tx.shard_state, compound_key)
      tx_drop_pending(compound_key)
      tx_mark_deleted(compound_key)
      send(self(), {:tx_pending_delete, compound_key})
      :ok
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_delete, redis_key, compound_key})
    end
  end

  def compound_delete(store, redis_key, compound_key) when is_map(store),
    do: store.compound_delete.(redis_key, compound_key)

  @spec compound_scan(store(), binary(), binary()) :: [{binary(), binary()}]
  def compound_scan(%FerricStore.Instance{} = ctx, redis_key, prefix),
    do: Router.compound_scan(ctx, redis_key, prefix)

  def compound_scan(%LocalTxStore{} = tx, redis_key, prefix) do
    if local?(tx, redis_key) do
      shard_data_path = promoted_path(tx, redis_key) || tx.shard_state.shard_data_path
      results = ShardETS.prefix_scan_entries(tx.shard_state, prefix, shard_data_path)

      results
      |> merge_tx_pending_prefix(prefix)
      |> Enum.sort_by(fn {field, _} -> field end)
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_scan, redis_key, prefix})
    end
  end

  def compound_scan(store, redis_key, prefix) when is_map(store),
    do: store.compound_scan.(redis_key, prefix)

  @spec compound_count(store(), binary(), binary()) :: non_neg_integer()
  def compound_count(%FerricStore.Instance{} = ctx, redis_key, prefix),
    do: Router.compound_count(ctx, redis_key, prefix)

  def compound_count(%LocalTxStore{} = tx, redis_key, prefix) do
    if local?(tx, redis_key) do
      ShardETS.prefix_count_entries(tx.shard_state, prefix)
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_count, redis_key, prefix})
    end
  end

  def compound_count(store, redis_key, prefix) when is_map(store),
    do: store.compound_count.(redis_key, prefix)

  @spec compound_delete_prefix(store(), binary(), binary()) :: :ok
  def compound_delete_prefix(%FerricStore.Instance{} = ctx, redis_key, prefix),
    do: Router.compound_delete_prefix(ctx, redis_key, prefix)

  def compound_delete_prefix(%LocalTxStore{} = tx, redis_key, prefix) do
    if local?(tx, redis_key) do
      keys_to_delete = ShardETS.prefix_collect_keys(tx.shard_state.keydir, prefix)

      Enum.each(keys_to_delete, fn key ->
        ShardETS.ets_delete_key(tx.shard_state, key)
        tx_drop_pending(key)
        tx_mark_deleted(key)
        send(self(), {:tx_pending_delete, key})
      end)

      :ok
    else
      shard = Router.resolve_shard(tx.instance_ctx, Router.shard_for(tx.instance_ctx, redis_key))
      GenServer.call(shard, {:compound_delete_prefix, redis_key, prefix})
    end
  end

  def compound_delete_prefix(store, redis_key, prefix) when is_map(store),
    do: store.compound_delete_prefix.(redis_key, prefix)

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

  def prob_dir(store, _key) when is_map(store) and is_map_key(store, :prob_dir),
    do: store.prob_dir.()

  def prob_dir(store, key) when is_map(store) and is_map_key(store, :prob_dir_for_key),
    do: store.prob_dir_for_key.(key)

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

  defp stored_value_size(value) when is_binary(value), do: byte_size(value)
  defp stored_value_size(value) when is_integer(value), do: byte_size(Integer.to_string(value))
  defp stored_value_size(value) when is_float(value), do: byte_size(Float.to_string(value))
  defp stored_value_size(value), do: value |> to_string() |> byte_size()

  defp tx_deleted?(key) do
    deleted = Process.get(:tx_deleted_keys, MapSet.new())
    MapSet.member?(deleted, key)
  end

  defp tx_pending_meta(key) do
    pending = Process.get(:tx_pending_values, %{})

    case Map.get(pending, key) do
      {value, 0} ->
        {value, 0}

      {value, exp} ->
        if exp > HLC.now_ms() do
          {value, exp}
        else
          tx_drop_pending(key)
          nil
        end

      nil ->
        nil
    end
  end

  defp tx_put_pending(key, value, expire_at_ms) do
    pending = Process.get(:tx_pending_values, %{})
    Process.put(:tx_pending_values, Map.put(pending, key, {value, expire_at_ms}))
    tx_undelete(key)
  end

  defp tx_drop_pending(key) do
    pending = Process.get(:tx_pending_values, %{})
    Process.put(:tx_pending_values, Map.delete(pending, key))
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
    case tx_pending_meta(key) do
      {value, _exp} -> value
      nil -> local_read_value_from_ets(tx, key)
    end
  end

  defp local_read_value_from_ets(tx, key) do
    case ShardETS.ets_lookup_warm(tx.shard_state, key) do
      {:hit, value, _exp} ->
        value

      :expired ->
        nil

      :miss ->
        case ShardReads.v2_local_read(tx.shard_state, key) do
          {:ok, nil} ->
            nil

          {:ok, value} ->
            ShardETS.ets_insert(tx.shard_state, key, value, 0)
            value

          _error ->
            nil
        end
    end
  end

  # Read {value, expire_at_ms} from local ETS, cold-read fallback. Returns {value, exp} or nil.
  defp local_read_meta(tx, key) do
    case tx_pending_meta(key) do
      {value, exp} -> {value, exp}
      nil -> local_read_meta_from_ets(tx, key)
    end
  end

  defp local_read_meta_from_ets(tx, key) do
    case ShardETS.ets_lookup_warm(tx.shard_state, key) do
      {:hit, value, exp} ->
        {value, exp}

      :expired ->
        nil

      :miss ->
        case ShardReads.v2_local_read(tx.shard_state, key) do
          {:ok, nil} ->
            nil

          {:ok, value} ->
            ShardETS.ets_insert(tx.shard_state, key, value, 0)
            {value, 0}

          _error ->
            nil
        end
    end
  end

  defp local_batch_get(tx, keys) do
    {local_entries, remote_entries} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({[], []}, fn {key, index}, {local_acc, remote_acc} ->
        if local?(tx, key) do
          if tx_deleted?(key),
            do: {local_acc, remote_acc},
            else: {[{index, key} | local_acc], remote_acc}
        else
          {local_acc, [{index, key} | remote_acc]}
        end
      end)

    results =
      local_entries
      |> Enum.reverse()
      |> local_batch_results(%{}, fn entries ->
        entries
        |> Enum.map(fn {_index, key} -> key end)
        |> then(&local_batch_read_values(tx, &1, tx.shard_state.shard_data_path))
      end)

    results =
      remote_entries
      |> Enum.reverse()
      |> local_batch_results(results, fn entries ->
        entries
        |> Enum.map(fn {_index, key} -> key end)
        |> then(&Router.batch_get(tx.instance_ctx, &1))
      end)

    keys
    |> Enum.with_index()
    |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
  end

  defp local_batch_results([], results, _read_fun), do: results

  defp local_batch_results(entries, results, read_fun) do
    entries
    |> Enum.zip(read_fun.(entries))
    |> Enum.reduce(results, fn {{index, _key}, value}, acc ->
      Map.put(acc, index, value)
    end)
  end

  defp local_batch_read_values(tx, keys, data_path) do
    tx
    |> local_batch_read_meta(keys, data_path)
    |> Enum.map(fn
      {value, _exp} -> value
      nil -> nil
    end)
  end

  defp local_batch_read_meta(tx, keys, data_path) do
    now = HLC.now_ms()

    {warm_results, cold_reads} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {key, index}, {results, cold} ->
        case tx_pending_meta(key) do
          {value, exp} ->
            {Map.put(results, index, {value, exp}), cold}

          nil ->
            local_batch_collect_ets(tx, key, index, data_path, now, results, cold)
        end
      end)

    results = local_batch_read_cold(tx, warm_results, Enum.reverse(cold_reads))

    keys
    |> Enum.with_index()
    |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
  end

  defp local_batch_collect_ets(tx, key, index, data_path, now, results, cold) do
    case :ets.lookup(tx.shard_state.keydir, key) do
      [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
        {Map.put(results, index, {value, 0}), cold}

      [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        {Map.put(results, index, {value, exp}), cold}

      [{^key, nil, 0, _lfu, fid, off, vsize}]
      when valid_cold_location(fid, off, vsize) ->
        path = ShardETS.file_path(data_path, fid)
        {results, [{index, key, path, fid, off, vsize, 0} | cold]}

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when exp > now and valid_cold_location(fid, off, vsize) ->
        path = ShardETS.file_path(data_path, fid)
        {results, [{index, key, path, fid, off, vsize, exp} | cold]}

      [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
        ShardETS.ets_delete_key(tx.shard_state, key)
        {results, cold}

      _ ->
        {results, cold}
    end
  rescue
    ArgumentError ->
      {results, cold}
  end

  defp local_batch_read_cold(_tx, results, []), do: results

  defp local_batch_read_cold(tx, results, cold_reads) do
    locations =
      Enum.map(cold_reads, fn {_index, key, path, _fid, off, _vsize, _exp} -> {path, off, key} end)

    case ColdRead.pread_batch_keyed(locations, @cold_read_timeout_ms) do
      {:ok, values} when is_list(values) and length(values) == length(cold_reads) ->
        cold_reads
        |> Enum.zip(values)
        |> Enum.reduce(results, fn
          {{index, key, _path, fid, off, vsize, exp}, value}, acc when is_binary(value) ->
            ShardETS.cold_read_warm_ets(tx.shard_state, key, value, exp, fid, off, vsize)
            Map.put(acc, index, {value, exp})

          {{_index, _key, path, _fid, _off, _vsize, _exp}, {:error, reason}}, acc ->
            ColdRead.emit_pread_error(path, reason)
            acc

          {_read, _missing_or_error}, acc ->
            acc
        end)

      {:ok, _bad_values} ->
        emit_local_batch_cold_errors(cold_reads, :batch_result_length_mismatch)
        results

      {:error, reason} ->
        emit_local_batch_cold_errors(cold_reads, reason)
        results
    end
  end

  defp emit_local_batch_cold_errors(cold_reads, reason) do
    cold_reads
    |> Enum.reduce(%{}, fn {_index, _key, path, _fid, _off, _vsize, _exp}, acc ->
      Map.update(acc, path, 1, &(&1 + 1))
    end)
    |> Enum.each(fn {path, count} -> ColdRead.emit_pread_error(path, reason, count) end)
  end

  defp local_set(tx, key, value, opts) do
    get? = Map.get(opts, :get, false)
    current = local_set_current_meta(tx, key, get?)

    {old_value, effective_expire} =
      case current do
        nil ->
          {nil, opts.expire_at_ms}

        {old_val, old_exp} ->
          {old_val, if(opts.keepttl, do: old_exp, else: opts.expire_at_ms)}
      end

    skip? =
      cond do
        opts.nx and current != nil -> true
        opts.xx and current == nil -> true
        true -> false
      end

    if skip? do
      if get?, do: old_value, else: nil
    else
      ShardETS.ets_insert(tx.shard_state, key, value, effective_expire)
      tx_put_pending(key, value, effective_expire)
      tx_undelete(key)
      send(self(), {:tx_pending_write, key, value, effective_expire})
      if get?, do: old_value, else: :ok
    end
  end

  defp fallback_set(store, key, value, opts) do
    get? = Map.get(opts, :get, false)
    current = fallback_set_current_meta(store, key, get?, opts.keepttl)

    {old_value, effective_expire} =
      case current do
        nil ->
          {nil, opts.expire_at_ms}

        {old_val, old_exp} ->
          {old_val, if(opts.keepttl, do: old_exp, else: opts.expire_at_ms)}
      end

    skip? =
      cond do
        opts.nx and exists?(store, key) -> true
        opts.xx and not exists?(store, key) -> true
        true -> false
      end

    if skip? do
      if get?, do: old_value, else: nil
    else
      put(store, key, value, effective_expire)
      if get?, do: old_value, else: :ok
    end
  end

  defp local_set_current_meta(tx, key, true), do: local_read_meta(tx, key)

  defp local_set_current_meta(tx, key, false) do
    if tx_deleted?(key) do
      nil
    else
      case tx_pending_meta(key) do
        {_value, _exp} = pending -> pending_expire_meta(pending)
        nil -> ets_expire_meta(tx, key)
      end
    end
  end

  defp pending_expire_meta({_value, exp}), do: {nil, exp}

  defp ets_expire_meta(tx, key) do
    case ShardETS.ets_lookup(tx.shard_state, key) do
      {:hit, _value, exp} -> {nil, exp}
      {:cold, _fid, _off, _vsize, exp} -> {nil, exp}
      _ -> nil
    end
  end

  defp fallback_set_current_meta(store, key, true, _keepttl), do: get_meta(store, key)

  defp fallback_set_current_meta(store, key, false, true) do
    case expire_at_ms(store, key) do
      nil -> nil
      exp -> {nil, exp}
    end
  end

  defp fallback_set_current_meta(_store, _key, false, false), do: nil

  # Read value for read-modify-write ops (incr, getset, getdel, getex).
  # Same as local_read_value but without warming ETS on cold read (matching original closures).
  defp local_read_value_for_rmw(tx, key) do
    case tx_pending_meta(key) do
      {value, _exp} -> value
      nil -> local_read_value_for_rmw_from_ets(tx, key)
    end
  end

  defp local_read_value_for_rmw_from_ets(tx, key) do
    case ShardETS.ets_lookup_warm(tx.shard_state, key) do
      {:hit, value, _exp} ->
        value

      :expired ->
        nil

      :miss ->
        case ShardReads.v2_local_read(tx.shard_state, key) do
          {:ok, nil} -> nil
          {:ok, v} -> v
          _ -> nil
        end
    end
  end

  defp promoted_path(%LocalTxStore{} = tx, redis_key) do
    case tx.shard_state.promoted_instances do
      %{^redis_key => %{path: path}} -> path
      _ -> nil
    end
  end

  defp local_promoted_read_value(tx, compound_key, dedicated_path) do
    case tx_pending_meta(compound_key) ||
           local_promoted_read_meta(tx, compound_key, dedicated_path) do
      {value, _exp} -> value
      nil -> nil
    end
  end

  defp local_promoted_read_meta(tx, compound_key, dedicated_path) do
    case tx_pending_meta(compound_key) do
      nil -> local_promoted_read_meta_from_ets(tx, compound_key, dedicated_path)
      meta -> meta
    end
  end

  defp local_promoted_read_meta_from_ets(tx, compound_key, dedicated_path) do
    now = HLC.now_ms()
    keydir = tx.shard_state.keydir

    case :ets.lookup(keydir, compound_key) do
      [{^compound_key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
        {value, 0}

      [{^compound_key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        {value, exp}

      [{^compound_key, nil, 0, _lfu, fid, off, vsize}]
      when valid_cold_location(fid, off, vsize) ->
        read_promoted_cold_value(tx, compound_key, dedicated_path, fid, off, vsize, 0)

      [{^compound_key, nil, exp, _lfu, fid, off, vsize}]
      when exp > now and valid_cold_location(fid, off, vsize) ->
        read_promoted_cold_value(tx, compound_key, dedicated_path, fid, off, vsize, exp)

      [{^compound_key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
        ShardETS.ets_delete_key(tx.shard_state, compound_key)
        nil

      _ ->
        nil
    end
  end

  defp merge_tx_pending_prefix(results, prefix) do
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
             (exp == 0 or exp > HLC.now_ms()) do
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
  end

  defp read_promoted_cold_value(tx, compound_key, dedicated_path, fid, off, vsize, exp) do
    path = ShardETS.file_path(dedicated_path, fid)

    case read_cold_async(path, off, compound_key) do
      {:ok, value} ->
        ShardETS.cold_read_warm_ets(tx.shard_state, compound_key, value, exp, fid, off, vsize)
        {value, exp}

      _ ->
        nil
    end
  end

  defp read_cold_async(path, offset, key) do
    Ferricstore.Store.ColdRead.pread_at(path, offset, key, @cold_read_timeout_ms)
  end
end
