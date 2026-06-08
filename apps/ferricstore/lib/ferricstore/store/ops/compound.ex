defmodule Ferricstore.Store.Ops.Compound do
  @moduledoc false

  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Store.Ops.LocalRead
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.ZSetIndex

  @typep store :: FerricStore.Instance.t() | LocalTxStore.t() | map()

  # --- Compound key operations ---

  @spec compound_get(store(), binary(), binary()) :: binary() | nil
  def compound_get(%FerricStore.Instance{} = ctx, redis_key, compound_key),
    do: Router.compound_get(ctx, redis_key, compound_key)

  def compound_get(%LocalTxStore{} = tx, redis_key, compound_key) do
    if LocalRead.local?(tx, redis_key) do
      case LocalRead.promoted_path(tx, redis_key) do
        nil ->
          LocalRead.local_read_value(tx, compound_key)

        dedicated_path ->
          if LocalRead.shared_log_compound_key?(compound_key) do
            LocalRead.local_read_value(tx, compound_key)
          else
            LocalRead.local_promoted_read_value(tx, compound_key, dedicated_path)
          end
      end
    else
      idx = Router.shard_for(tx.instance_ctx, redis_key)

      case Router.safe_read_call(tx.instance_ctx, idx, {:compound_get, redis_key, compound_key}) do
        {:ok, value} -> value
        :unavailable -> nil
      end
    end
  end

  def compound_get(store, redis_key, compound_key) when is_map(store),
    do: store.compound_get.(redis_key, compound_key)

  @spec compound_batch_get(store(), binary(), [binary()]) :: [binary() | nil]
  def compound_batch_get(%FerricStore.Instance{} = ctx, redis_key, compound_keys),
    do: Router.compound_batch_get(ctx, redis_key, compound_keys)

  def compound_batch_get(%LocalTxStore{} = tx, redis_key, compound_keys) do
    if LocalRead.local?(tx, redis_key) do
      case LocalRead.promoted_path(tx, redis_key) do
        nil ->
          LocalRead.local_batch_read_values(tx, compound_keys, tx.shard_state.shard_data_path)

        dedicated_path ->
          if Enum.any?(compound_keys, &LocalRead.shared_log_compound_key?/1) do
            LocalRead.local_promoted_batch_read_values(tx, compound_keys, dedicated_path)
          else
            LocalRead.local_batch_read_values(tx, compound_keys, dedicated_path)
          end
      end
    else
      idx = Router.shard_for(tx.instance_ctx, redis_key)

      case Router.safe_read_call(
             tx.instance_ctx,
             idx,
             {:compound_batch_get, redis_key, compound_keys}
           ) do
        {:ok, values} -> values
        :unavailable -> List.duplicate(nil, length(compound_keys))
      end
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
    if LocalRead.local?(tx, redis_key) do
      case LocalRead.promoted_path(tx, redis_key) do
        nil ->
          LocalRead.local_read_meta(tx, compound_key)

        dedicated_path ->
          if LocalRead.shared_log_compound_key?(compound_key) do
            LocalRead.local_read_meta(tx, compound_key)
          else
            LocalRead.local_promoted_read_meta(tx, compound_key, dedicated_path)
          end
      end
    else
      idx = Router.shard_for(tx.instance_ctx, redis_key)

      case Router.safe_read_call(
             tx.instance_ctx,
             idx,
             {:compound_get_meta, redis_key, compound_key}
           ) do
        {:ok, meta} -> meta
        :unavailable -> nil
      end
    end
  end

  def compound_get_meta(store, redis_key, compound_key) when is_map(store),
    do: store.compound_get_meta.(redis_key, compound_key)

  @spec compound_batch_get_meta(store(), binary(), [binary()]) ::
          [{binary(), non_neg_integer()} | nil]
  def compound_batch_get_meta(%FerricStore.Instance{} = ctx, redis_key, compound_keys),
    do: Router.compound_batch_get_meta(ctx, redis_key, compound_keys)

  def compound_batch_get_meta(%LocalTxStore{} = tx, redis_key, compound_keys) do
    if LocalRead.local?(tx, redis_key) do
      case LocalRead.promoted_path(tx, redis_key) do
        nil ->
          LocalRead.local_batch_read_meta(tx, compound_keys, tx.shard_state.shard_data_path)

        dedicated_path ->
          if Enum.any?(compound_keys, &LocalRead.shared_log_compound_key?/1) do
            LocalRead.local_promoted_batch_read_meta(tx, compound_keys, dedicated_path)
          else
            LocalRead.local_batch_read_meta(tx, compound_keys, dedicated_path)
          end
      end
    else
      idx = Router.shard_for(tx.instance_ctx, redis_key)

      case Router.safe_read_call(
             tx.instance_ctx,
             idx,
             {:compound_batch_get_meta, redis_key, compound_keys}
           ) do
        {:ok, metas} -> metas
        :unavailable -> List.duplicate(nil, length(compound_keys))
      end
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
    if LocalRead.local?(tx, redis_key) do
      ShardETS.ets_insert(tx.shard_state, compound_key, value, expire_at_ms)
      LocalRead.tx_put_pending(compound_key, value, expire_at_ms)
      LocalRead.tx_undelete(compound_key)

      send(
        self(),
        LocalRead.tx_compound_write_message(tx, redis_key, compound_key, value, expire_at_ms)
      )

      :ok
    else
      Router.compound_put(tx.instance_ctx, redis_key, compound_key, value, expire_at_ms)
    end
  end

  def compound_put(store, redis_key, compound_key, value, exp) when is_map(store),
    do: store.compound_put.(redis_key, compound_key, value, exp)

  @spec compound_batch_put(store(), binary(), [{binary(), binary(), non_neg_integer()}]) :: :ok
  def compound_batch_put(_store, _redis_key, []), do: :ok

  def compound_batch_put(%FerricStore.Instance{} = ctx, redis_key, entries),
    do: Router.compound_batch_put(ctx, redis_key, entries)

  def compound_batch_put(%LocalTxStore{} = tx, redis_key, entries) do
    if LocalRead.local?(tx, redis_key) do
      Enum.each(entries, fn {compound_key, value, expire_at_ms} ->
        ShardETS.ets_insert(tx.shard_state, compound_key, value, expire_at_ms)
        LocalRead.tx_put_pending(compound_key, value, expire_at_ms)
        LocalRead.tx_undelete(compound_key)

        send(
          self(),
          LocalRead.tx_compound_write_message(tx, redis_key, compound_key, value, expire_at_ms)
        )
      end)

      :ok
    else
      Router.compound_batch_put(tx.instance_ctx, redis_key, entries)
    end
  end

  def compound_batch_put(store, redis_key, entries) when is_map(store) do
    case store do
      %{compound_batch_put: compound_batch_put_fun} when is_function(compound_batch_put_fun, 2) ->
        compound_batch_put_fun.(redis_key, entries)

      _ ->
        Enum.reduce_while(entries, :ok, fn {compound_key, value, expire_at_ms}, :ok ->
          case compound_put(store, redis_key, compound_key, value, expire_at_ms) do
            :ok -> {:cont, :ok}
            {:error, _} = err -> {:halt, err}
            other -> {:halt, {:error, inspect(other)}}
          end
        end)
    end
  end

  @spec compound_delete(store(), binary(), binary()) :: :ok
  def compound_delete(%FerricStore.Instance{} = ctx, redis_key, compound_key),
    do: Router.compound_delete(ctx, redis_key, compound_key)

  def compound_delete(%LocalTxStore{} = tx, redis_key, compound_key) do
    if LocalRead.local?(tx, redis_key) do
      ShardETS.ets_delete_key(tx.shard_state, compound_key)
      LocalRead.tx_drop_pending(compound_key)
      LocalRead.tx_mark_deleted(compound_key)
      send(self(), LocalRead.tx_compound_delete_message(tx, redis_key, compound_key))
      :ok
    else
      Router.compound_delete(tx.instance_ctx, redis_key, compound_key)
    end
  end

  def compound_delete(store, redis_key, compound_key) when is_map(store),
    do: store.compound_delete.(redis_key, compound_key)

  @spec compound_batch_delete(store(), binary(), [binary()]) :: :ok | {:error, term()}
  def compound_batch_delete(_store, _redis_key, []), do: :ok

  def compound_batch_delete(%FerricStore.Instance{} = ctx, redis_key, compound_keys),
    do: Router.compound_batch_delete(ctx, redis_key, compound_keys)

  def compound_batch_delete(%LocalTxStore{} = tx, redis_key, compound_keys) do
    if LocalRead.local?(tx, redis_key) do
      Enum.each(compound_keys, fn compound_key ->
        ShardETS.ets_delete_key(tx.shard_state, compound_key)
        LocalRead.tx_drop_pending(compound_key)
        LocalRead.tx_mark_deleted(compound_key)
        send(self(), LocalRead.tx_compound_delete_message(tx, redis_key, compound_key))
      end)

      :ok
    else
      Router.compound_batch_delete(tx.instance_ctx, redis_key, compound_keys)
    end
  end

  def compound_batch_delete(store, redis_key, compound_keys) when is_map(store) do
    case store do
      %{compound_batch_delete: compound_batch_delete_fun}
      when is_function(compound_batch_delete_fun, 2) ->
        compound_batch_delete_fun.(redis_key, compound_keys)

      _ ->
        Enum.reduce_while(compound_keys, :ok, fn compound_key, :ok ->
          case compound_delete(store, redis_key, compound_key) do
            :ok -> {:cont, :ok}
            {:error, _} = err -> {:halt, err}
            other -> {:halt, {:error, inspect(other)}}
          end
        end)
    end
  end

  @spec compound_scan(store(), binary(), binary()) :: [{binary(), binary()}]
  def compound_scan(%FerricStore.Instance{} = ctx, redis_key, prefix),
    do: Router.compound_scan(ctx, redis_key, prefix)

  def compound_scan(%LocalTxStore{} = tx, redis_key, prefix) do
    if LocalRead.local?(tx, redis_key) do
      shard_data_path = LocalRead.promoted_path(tx, redis_key) || tx.shard_state.shard_data_path
      results = ShardETS.prefix_scan_entries(tx.shard_state, prefix, shard_data_path)

      results
      |> LocalRead.merge_tx_pending_prefix(prefix)
      |> Enum.sort_by(fn {field, _} -> field end)
    else
      idx = Router.shard_for(tx.instance_ctx, redis_key)

      case Router.safe_read_call(tx.instance_ctx, idx, {:compound_scan, redis_key, prefix}) do
        {:ok, results} -> results
        :unavailable -> []
      end
    end
  end

  def compound_scan(store, redis_key, prefix) when is_map(store),
    do: store.compound_scan.(redis_key, prefix)

  @spec compound_fields(store(), binary(), binary()) :: [binary()]
  def compound_fields(%FerricStore.Instance{} = ctx, redis_key, prefix),
    do: Router.compound_fields(ctx, redis_key, prefix)

  def compound_fields(store, redis_key, prefix) do
    store
    |> compound_scan(redis_key, prefix)
    |> Enum.map(fn {field, _value} -> field end)
  end

  @spec compound_count(store(), binary(), binary()) :: non_neg_integer()
  def compound_count(%FerricStore.Instance{} = ctx, redis_key, prefix),
    do: Router.compound_count(ctx, redis_key, prefix)

  def compound_count(%LocalTxStore{} = tx, redis_key, prefix) do
    if LocalRead.local?(tx, redis_key) do
      ShardETS.prefix_count_entries(tx.shard_state, prefix)
    else
      idx = Router.shard_for(tx.instance_ctx, redis_key)

      case Router.safe_read_call(tx.instance_ctx, idx, {:compound_count, redis_key, prefix}) do
        {:ok, count} -> count
        :unavailable -> 0
      end
    end
  end

  def compound_count(store, redis_key, prefix) when is_map(store),
    do: store.compound_count.(redis_key, prefix)

  @spec zset_score_range(store(), binary(), term(), term(), boolean()) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_score_range(%FerricStore.Instance{} = ctx, redis_key, min_bound, max_bound, reverse?),
    do: Router.zset_score_range(ctx, redis_key, min_bound, max_bound, reverse?)

  def zset_score_range(%LocalTxStore{} = tx, redis_key, min_bound, max_bound, reverse?) do
    LocalRead.local_zset_index_read(tx, redis_key, fn state ->
      {:ok, ZSetIndex.range(state.zset_score_index, redis_key, min_bound, max_bound, reverse?)}
    end)
  end

  def zset_score_range(store, redis_key, min_bound, max_bound, reverse?) when is_map(store) do
    case store do
      %{zset_score_range: fun} when is_function(fun, 4) ->
        fun.(redis_key, min_bound, max_bound, reverse?)

      _ ->
        :unavailable
    end
  end

  @spec zset_score_range_slice(
          store(),
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_score_range_slice(
        %FerricStore.Instance{} = ctx,
        redis_key,
        min_bound,
        max_bound,
        reverse?,
        offset,
        count
      ),
      do:
        Router.zset_score_range_slice(
          ctx,
          redis_key,
          min_bound,
          max_bound,
          reverse?,
          offset,
          count
        )

  def zset_score_range_slice(
        %LocalTxStore{} = tx,
        redis_key,
        min_bound,
        max_bound,
        reverse?,
        offset,
        count
      ) do
    LocalRead.local_zset_index_read(tx, redis_key, fn state ->
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
  end

  def zset_score_range_slice(store, redis_key, min_bound, max_bound, reverse?, offset, count)
      when is_map(store) do
    case store do
      %{zset_score_range_slice: fun} when is_function(fun, 6) ->
        fun.(redis_key, min_bound, max_bound, reverse?, offset, count)

      _ ->
        :unavailable
    end
  end

  @spec zset_score_count(store(), binary(), term(), term()) ::
          {:ok, non_neg_integer()} | :unavailable
  def zset_score_count(%FerricStore.Instance{} = ctx, redis_key, min_bound, max_bound),
    do: Router.zset_score_count(ctx, redis_key, min_bound, max_bound)

  def zset_score_count(%LocalTxStore{} = tx, redis_key, min_bound, max_bound) do
    LocalRead.local_zset_index_read(tx, redis_key, fn state ->
      {:ok,
       ZSetIndex.count(
         state.zset_score_index,
         state.zset_score_lookup,
         redis_key,
         min_bound,
         max_bound
       )}
    end)
  end

  def zset_score_count(store, redis_key, min_bound, max_bound) when is_map(store) do
    case store do
      %{zset_score_count: fun} when is_function(fun, 3) ->
        fun.(redis_key, min_bound, max_bound)

      _ ->
        :unavailable
    end
  end

  @spec zset_rank_range(store(), binary(), non_neg_integer(), non_neg_integer(), boolean()) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_rank_range(%FerricStore.Instance{} = ctx, redis_key, start_idx, stop_idx, reverse?),
    do: Router.zset_rank_range(ctx, redis_key, start_idx, stop_idx, reverse?)

  def zset_rank_range(%LocalTxStore{} = tx, redis_key, start_idx, stop_idx, reverse?) do
    LocalRead.local_zset_index_read(tx, redis_key, fn state ->
      {:ok,
       ZSetIndex.rank_range(state.zset_score_index, redis_key, start_idx, stop_idx, reverse?)}
    end)
  end

  def zset_rank_range(store, redis_key, start_idx, stop_idx, reverse?) when is_map(store) do
    case store do
      %{zset_rank_range: fun} when is_function(fun, 4) ->
        fun.(redis_key, start_idx, stop_idx, reverse?)

      _ ->
        :unavailable
    end
  end

  @spec zset_member_rank(store(), binary(), binary(), boolean()) ::
          {:ok, non_neg_integer() | nil} | :unavailable
  def zset_member_rank(%FerricStore.Instance{} = ctx, redis_key, member, reverse?),
    do: Router.zset_member_rank(ctx, redis_key, member, reverse?)

  def zset_member_rank(%LocalTxStore{} = tx, redis_key, member, reverse?) do
    LocalRead.local_zset_index_read(tx, redis_key, fn state ->
      {:ok,
       ZSetIndex.member_rank(
         state.zset_score_index,
         state.zset_score_lookup,
         redis_key,
         member,
         reverse?
       )}
    end)
  end

  def zset_member_rank(store, redis_key, member, reverse?) when is_map(store) do
    case store do
      %{zset_member_rank: fun} when is_function(fun, 3) ->
        fun.(redis_key, member, reverse?)

      _ ->
        :unavailable
    end
  end

  @spec compound_delete_prefix(store(), binary(), binary()) :: :ok
  def compound_delete_prefix(%FerricStore.Instance{} = ctx, redis_key, prefix),
    do: Router.compound_delete_prefix(ctx, redis_key, prefix)

  def compound_delete_prefix(%LocalTxStore{} = tx, redis_key, prefix) do
    if LocalRead.local?(tx, redis_key) do
      keys_to_delete = ShardETS.prefix_collect_keys(tx.shard_state.keydir, prefix)

      Enum.each(keys_to_delete, fn key ->
        ShardETS.ets_delete_key(tx.shard_state, key)
        LocalRead.tx_drop_pending(key)
        LocalRead.tx_mark_deleted(key)
        send(self(), LocalRead.tx_compound_delete_message(tx, redis_key, key))
      end)

      :ok
    else
      Router.compound_delete_prefix(tx.instance_ctx, redis_key, prefix)
    end
  end

  def compound_delete_prefix(store, redis_key, prefix) when is_map(store),
    do: store.compound_delete_prefix.(redis_key, prefix)
end
