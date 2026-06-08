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
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Writes, as: ShardWrites
  alias Ferricstore.Store.Ops.Compound, as: CompoundOps
  alias Ferricstore.Store.Ops.{Delete, Flush, LocalRead, MapStore}

  @typep store :: FerricStore.Instance.t() | LocalTxStore.t() | map()
  @max_int64 9_223_372_036_854_775_807
  @min_int64 -9_223_372_036_854_775_808
  @overflow_error "ERR increment or decrement would overflow"

  defguardp valid_cold_location(file_id, offset, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
                   is_integer(value_size) and value_size >= 0

  # --- Basic key operations ---

  @spec get(store(), binary()) :: binary() | nil
  def get(%FerricStore.Instance{} = ctx, key), do: Router.get(ctx, key)

  def get(%LocalTxStore{} = tx, key) do
    if LocalRead.local?(tx, key) do
      if LocalRead.tx_deleted?(key), do: nil, else: LocalRead.local_read_value(tx, key)
    else
      Router.get(tx.instance_ctx, key)
    end
  end

  def get(store, key) when is_map(store), do: store.get.(key)

  @spec batch_get(store(), [binary()]) :: [binary() | nil]
  def batch_get(%FerricStore.Instance{} = ctx, keys), do: Router.batch_get(ctx, keys)

  def batch_get(%LocalTxStore{} = tx, keys), do: LocalRead.local_batch_get(tx, keys)

  def batch_get(store, keys) when is_map(store) do
    case store do
      %{batch_get: batch_get_fun} when is_function(batch_get_fun, 1) ->
        batch_get_fun.(keys)

      _ ->
        Enum.map(keys, &get(store, &1))
    end
  end

  @spec batch_put(store(), [{binary(), binary()}]) :: :ok | {:error, term()}
  def batch_put(_store, []), do: :ok

  def batch_put(%FerricStore.Instance{} = ctx, kv_pairs),
    do: Router.batch_put(ctx, kv_pairs)

  def batch_put(%LocalTxStore{} = tx, kv_pairs) do
    Enum.reduce_while(kv_pairs, :ok, fn {key, value}, :ok ->
      case put(tx, key, value, 0) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
        other -> {:halt, {:error, inspect(other)}}
      end
    end)
  end

  def batch_put(store, kv_pairs) when is_map(store) do
    case store do
      %{batch_put: batch_put_fun} when is_function(batch_put_fun, 1) ->
        batch_put_fun.(kv_pairs)

      _ ->
        Enum.reduce_while(kv_pairs, :ok, fn {key, value}, :ok ->
          case put(store, key, value, 0) do
            :ok -> {:cont, :ok}
            {:error, _} = err -> {:halt, err}
            other -> {:halt, {:error, inspect(other)}}
          end
        end)
    end
  end

  @spec get_meta(store(), binary()) :: {binary(), non_neg_integer()} | nil
  def get_meta(%FerricStore.Instance{} = ctx, key), do: Router.get_meta(ctx, key)

  def get_meta(%LocalTxStore{} = tx, key) do
    if LocalRead.local?(tx, key) do
      if LocalRead.tx_deleted?(key), do: nil, else: LocalRead.local_read_meta(tx, key)
    else
      Router.get_meta(tx.instance_ctx, key)
    end
  end

  def get_meta(store, key) when is_map(store), do: store.get_meta.(key)

  @spec expire_at_ms(store(), binary()) :: non_neg_integer() | nil
  def expire_at_ms(%FerricStore.Instance{} = ctx, key), do: Router.expire_at_ms(ctx, key)

  def expire_at_ms(%LocalTxStore{} = tx, key) do
    if LocalRead.local?(tx, key) do
      case LocalRead.tx_pending_meta(key) do
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
      not LocalRead.local?(tx, key) ->
        Router.value_size(tx.instance_ctx, key)

      LocalRead.tx_deleted?(key) ->
        nil

      true ->
        case LocalRead.tx_pending_meta(key) do
          {value, _exp} ->
            LocalRead.stored_value_size(value)

          nil ->
            case ShardETS.ets_lookup(tx.shard_state, key) do
              {:hit, value, _exp} ->
                LocalRead.stored_value_size(value)

              {:cold, fid, off, vsize, _exp} ->
                LocalRead.local_cold_value_size(tx, key, fid, off, vsize)

              _ ->
                nil
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
          value -> LocalRead.stored_value_size(value)
        end
    end
  end

  @spec object_lfu(store(), binary()) :: non_neg_integer() | nil
  def object_lfu(%FerricStore.Instance{} = ctx, key), do: Router.object_lfu(ctx, key)

  def object_lfu(%LocalTxStore{} = tx, key) do
    cond do
      not LocalRead.local?(tx, key) ->
        Router.object_lfu(tx.instance_ctx, key)

      LocalRead.tx_deleted?(key) ->
        nil

      LocalRead.tx_pending_meta(key) != nil ->
        Ferricstore.Store.LFU.initial()

      true ->
        now = HLC.now_ms()

        case :ets.lookup(tx.shard_state.keydir, key) do
          [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
            lfu

          [{^key, nil, 0, lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
            lfu

          [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
            lfu

          [{^key, nil, exp, lfu, fid, off, vsize}]
          when exp > now and valid_cold_location(fid, off, vsize) ->
            lfu

          _ ->
            nil
        end
    end
  end

  def object_lfu(store, key) when is_map(store) do
    case store do
      %{object_lfu: object_lfu} when is_function(object_lfu, 1) -> object_lfu.(key)
      _ -> nil
    end
  end

  @spec getrange(store(), binary(), integer(), integer()) :: binary() | nil
  def getrange(%FerricStore.Instance{} = ctx, key, start_idx, end_idx),
    do: Router.getrange(ctx, key, start_idx, end_idx)

  def getrange(%LocalTxStore{} = tx, key, start_idx, end_idx) do
    cond do
      not LocalRead.local?(tx, key) ->
        Router.getrange(tx.instance_ctx, key, start_idx, end_idx)

      LocalRead.tx_deleted?(key) ->
        nil

      true ->
        case LocalRead.tx_pending_meta(key) do
          {value, _exp} ->
            LocalRead.range_from_value(value, start_idx, end_idx)

          nil ->
            case ShardETS.ets_lookup(tx.shard_state, key) do
              {:hit, value, _exp} ->
                LocalRead.range_from_value(value, start_idx, end_idx)

              {:cold, _fid, _off, _vsize, _exp} ->
                Router.getrange(tx.instance_ctx, key, start_idx, end_idx)

              _ ->
                nil
            end
        end
    end
  end

  def getrange(store, key, start_idx, end_idx) when is_map(store) do
    case store do
      %{getrange: getrange_fun} when is_function(getrange_fun, 3) ->
        getrange_fun.(key, start_idx, end_idx)

      _ ->
        case get(store, key) do
          nil -> nil
          value -> LocalRead.range_from_value(value, start_idx, end_idx)
        end
    end
  end

  @spec put(store(), binary(), binary(), non_neg_integer()) :: :ok | {:error, binary()}
  def put(%FerricStore.Instance{} = ctx, key, value, exp), do: Router.put(ctx, key, value, exp)

  def put(%LocalTxStore{} = tx, key, value, exp) do
    if LocalRead.local?(tx, key) do
      ShardETS.ets_insert(tx.shard_state, key, value, exp)
      LocalRead.tx_put_pending(key, value, exp)
      LocalRead.tx_undelete(key)
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
    if LocalRead.local?(tx, key) do
      LocalRead.local_set(tx, key, value, opts)
    else
      Router.set(tx.instance_ctx, key, value, opts)
    end
  end

  def set(store, key, value, opts) when is_map(store) do
    case store do
      %{set: set_fun} when is_function(set_fun, 3) ->
        set_fun.(key, value, opts)

      _ ->
        MapStore.set(store, key, value, opts)
    end
  end

  @spec delete(store(), binary()) :: :ok
  def delete(store, key), do: Delete.delete(store, key)

  @spec exists?(store(), binary()) :: boolean()
  def exists?(%FerricStore.Instance{} = ctx, key), do: Router.exists?(ctx, key)

  def exists?(%LocalTxStore{} = tx, key) do
    if LocalRead.local?(tx, key) do
      LocalRead.local_exists?(tx, key)
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
    if LocalRead.local?(tx, key) do
      {current, expire_at_ms} = LocalRead.local_read_meta_for_rmw(tx, key)

      case current do
        nil ->
          case checked_integer_add(0, delta) do
            {:ok, new_val} ->
              ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
              LocalRead.tx_put_pending(key, new_val, 0)
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
                  ShardETS.ets_insert(tx.shard_state, key, new_val, expire_at_ms)
                  LocalRead.tx_put_pending(key, new_val, expire_at_ms)
                  send(self(), {:tx_pending_write, key, new_val, expire_at_ms})
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
    if LocalRead.local?(tx, key) do
      {current, expire_at_ms} = LocalRead.local_read_meta_for_rmw(tx, key)

      case current do
        nil ->
          new_val = delta * 1.0
          ShardETS.ets_insert(tx.shard_state, key, new_val, 0)
          LocalRead.tx_put_pending(key, new_val, 0)
          send(self(), {:tx_pending_write, key, new_val, 0})
          {:ok, new_val}

        value ->
          case ShardETS.coerce_float(value) do
            {:ok, float_val} ->
              new_val = float_val + delta
              ShardETS.ets_insert(tx.shard_state, key, new_val, expire_at_ms)
              LocalRead.tx_put_pending(key, new_val, expire_at_ms)
              send(self(), {:tx_pending_write, key, new_val, expire_at_ms})
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
    if LocalRead.local?(tx, key) do
      {current, expire_at_ms} =
        case LocalRead.local_read_meta_for_rmw(tx, key) do
          {nil, _exp} -> {"", 0}
          {value, exp} -> {ShardETS.to_disk_binary(value), exp}
        end

      new_val = current <> suffix
      ShardETS.ets_insert(tx.shard_state, key, new_val, expire_at_ms)
      LocalRead.tx_put_pending(key, new_val, expire_at_ms)
      send(self(), {:tx_pending_write, key, new_val, expire_at_ms})
      {:ok, byte_size(new_val)}
    else
      Router.append(tx.instance_ctx, key, suffix)
    end
  end

  def append(store, key, suffix) when is_map(store), do: store.append.(key, suffix)

  @spec getset(store(), binary(), binary()) :: binary() | nil
  def getset(%FerricStore.Instance{} = ctx, key, value), do: Router.getset(ctx, key, value)

  def getset(%LocalTxStore{} = tx, key, new_value) do
    if LocalRead.local?(tx, key) do
      old = LocalRead.local_read_value_for_rmw(tx, key)
      ShardETS.ets_insert(tx.shard_state, key, new_value, 0)
      LocalRead.tx_put_pending(key, new_value, 0)
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
    if LocalRead.local?(tx, key) do
      old = LocalRead.local_read_value_for_rmw(tx, key)

      if old do
        ShardETS.ets_delete_key(tx.shard_state, key)
        LocalRead.tx_drop_pending(key)
        LocalRead.tx_mark_deleted(key)
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
    if LocalRead.local?(tx, key) do
      value = LocalRead.local_read_value_for_rmw(tx, key)

      if value do
        ShardETS.ets_insert(tx.shard_state, key, value, expire_at_ms)
        LocalRead.tx_put_pending(key, value, expire_at_ms)
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
    if LocalRead.local?(tx, key) do
      {old, expire_at_ms} =
        case LocalRead.local_read_meta_for_rmw(tx, key) do
          {nil, _exp} -> {"", 0}
          {v, exp} -> {ShardETS.to_disk_binary(v), exp}
        end

      new_val = ShardWrites.apply_setrange(old, offset, value)
      ShardETS.ets_insert(tx.shard_state, key, new_val, expire_at_ms)
      LocalRead.tx_put_pending(key, new_val, expire_at_ms)
      send(self(), {:tx_pending_write, key, new_val, expire_at_ms})
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

  def compound_get(store, redis_key, compound_key),
    do: CompoundOps.compound_get(store, redis_key, compound_key)

  def compound_batch_get(store, redis_key, compound_keys),
    do: CompoundOps.compound_batch_get(store, redis_key, compound_keys)

  def compound_get_meta(store, redis_key, compound_key),
    do: CompoundOps.compound_get_meta(store, redis_key, compound_key)

  def compound_batch_get_meta(store, redis_key, compound_keys),
    do: CompoundOps.compound_batch_get_meta(store, redis_key, compound_keys)

  def compound_put(store, redis_key, compound_key, value, exp),
    do: CompoundOps.compound_put(store, redis_key, compound_key, value, exp)

  def compound_batch_put(store, redis_key, entries),
    do: CompoundOps.compound_batch_put(store, redis_key, entries)

  def compound_delete(store, redis_key, compound_key),
    do: CompoundOps.compound_delete(store, redis_key, compound_key)

  def compound_batch_delete(store, redis_key, compound_keys),
    do: CompoundOps.compound_batch_delete(store, redis_key, compound_keys)

  def compound_scan(store, redis_key, prefix),
    do: CompoundOps.compound_scan(store, redis_key, prefix)

  def compound_fields(store, redis_key, prefix),
    do: CompoundOps.compound_fields(store, redis_key, prefix)

  def compound_count(store, redis_key, prefix),
    do: CompoundOps.compound_count(store, redis_key, prefix)

  def zset_score_range(store, redis_key, min_bound, max_bound, reverse?),
    do: CompoundOps.zset_score_range(store, redis_key, min_bound, max_bound, reverse?)

  def zset_score_range_slice(store, redis_key, min_bound, max_bound, reverse?, offset, count),
    do:
      CompoundOps.zset_score_range_slice(
        store,
        redis_key,
        min_bound,
        max_bound,
        reverse?,
        offset,
        count
      )

  def zset_score_count(store, redis_key, min_bound, max_bound),
    do: CompoundOps.zset_score_count(store, redis_key, min_bound, max_bound)

  def zset_rank_range(store, redis_key, start_idx, stop_idx, reverse?),
    do: CompoundOps.zset_rank_range(store, redis_key, start_idx, stop_idx, reverse?)

  def zset_member_rank(store, redis_key, member, reverse?),
    do: CompoundOps.zset_member_rank(store, redis_key, member, reverse?)

  def compound_delete_prefix(store, redis_key, prefix),
    do: CompoundOps.compound_delete_prefix(store, redis_key, prefix)

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

  def prob_dir(store, key) when is_map(store) and is_map_key(store, :prob_dir_for_key),
    do: store.prob_dir_for_key.(key)

  def prob_dir(store, _key) when is_map(store) and is_map_key(store, :prob_dir),
    do: store.prob_dir.()

  # --- Flush ---

  @spec flush(store()) :: :ok | {:error, term()}
  def flush(%FerricStore.Instance{} = ctx), do: Flush.flush(ctx)
  def flush(%LocalTxStore{} = tx), do: Flush.flush(tx.instance_ctx)

  def flush(store) when is_map(store), do: store.flush.()

  # --- On push callback (for Waiters notification) ---

  @spec on_push(store(), binary(), non_neg_integer()) :: [pid()] | pid() | nil
  def on_push(store, key, count \\ 1) do
    case store do
      %FerricStore.Instance{} ->
        Ferricstore.Waiters.notify_push(key, count)

      %LocalTxStore{} ->
        Ferricstore.Waiters.notify_push(key, count)

      store when is_map(store) ->
        case store[:on_push] do
          fun when is_function(fun, 2) -> fun.(key, count)
          fun when is_function(fun, 1) -> fun.(key)
          _ -> nil
        end
    end
  end
end
