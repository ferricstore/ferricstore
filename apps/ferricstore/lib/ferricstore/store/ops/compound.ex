defmodule Ferricstore.Store.Ops.Compound do
  @moduledoc false

  alias Ferricstore.Store.{LocalTxStore, ReadResult}
  alias Ferricstore.Store.Ops.LocalRead
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.ZSetIndex

  @typep store :: FerricStore.Instance.t() | LocalTxStore.t() | map()

  # --- Compound key operations ---

  @spec compound_type_claim(store(), binary(), atom()) ::
          :unsupported | :ok | {:ok, :created} | {:error, term()}
  def compound_type_claim(%FerricStore.Instance{} = ctx, redis_key, type),
    do: Router.compound_type_claim(ctx, redis_key, type)

  def compound_type_claim(%LocalTxStore{}, _redis_key, _type), do: :unsupported

  def compound_type_claim(store, redis_key, type) when is_map(store) do
    case store do
      %{compound_type_claim: claim_fun} when is_function(claim_fun, 2) ->
        claim_fun.(redis_key, type)

      _without_atomic_claim ->
        :unsupported
    end
  end

  @spec compound_get(store(), binary(), binary()) :: binary() | nil | ReadResult.failure()
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
        :unavailable -> ReadResult.failure(:shard_unavailable)
      end
    end
  end

  def compound_get(store, redis_key, compound_key) when is_map(store),
    do: store.compound_get.(redis_key, compound_key)

  @spec compound_batch_get(store(), binary(), [binary()]) ::
          [binary() | nil | ReadResult.failure()]
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
        {:ok, values} ->
          normalize_shard_batch_reply(values, length(compound_keys))

        :unavailable ->
          List.duplicate(ReadResult.failure(:shard_unavailable), length(compound_keys))
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

  @spec compound_get_meta(store(), binary(), binary()) ::
          {binary(), non_neg_integer()} | nil | ReadResult.failure()
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
        :unavailable -> ReadResult.failure(:shard_unavailable)
      end
    end
  end

  def compound_get_meta(store, redis_key, compound_key) when is_map(store),
    do: store.compound_get_meta.(redis_key, compound_key)

  @spec compound_batch_get_meta(store(), binary(), [binary()]) ::
          [{binary(), non_neg_integer()} | nil | ReadResult.failure()]
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
        {:ok, metas} ->
          normalize_shard_batch_reply(metas, length(compound_keys))

        :unavailable ->
          List.duplicate(ReadResult.failure(:shard_unavailable), length(compound_keys))
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

  defp normalize_shard_batch_reply(values, expected_count)
       when is_list(values) and is_integer(expected_count) and expected_count >= 0 do
    if exact_batch_reply?(values, expected_count) do
      values
    else
      invalid_shard_batch_reply(expected_count)
    end
  end

  defp normalize_shard_batch_reply(_invalid, expected_count),
    do: invalid_shard_batch_reply(expected_count)

  defp exact_batch_reply?([], 0), do: true

  defp exact_batch_reply?([_value | rest], remaining) when remaining > 0,
    do: exact_batch_reply?(rest, remaining - 1)

  defp exact_batch_reply?(_values, _remaining), do: false

  defp invalid_shard_batch_reply(expected_count),
    do: List.duplicate(ReadResult.failure(:invalid_shard_batch_reply), expected_count)

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

  @spec compound_batch_mutate(
          store(),
          binary(),
          [binary()],
          [{binary(), binary(), non_neg_integer()}]
        ) :: :ok | {:error, term()}
  def compound_batch_mutate(_store, _redis_key, [], []), do: :ok

  def compound_batch_mutate(%LocalTxStore{} = tx, redis_key, compound_keys, entries) do
    with :ok <- compound_batch_delete(tx, redis_key, compound_keys),
         :ok <- compound_batch_put(tx, redis_key, entries) do
      :ok
    end
  end

  def compound_batch_mutate(%FerricStore.Instance{} = ctx, redis_key, compound_keys, entries) do
    compound_batch_mutate_fallback(ctx, redis_key, compound_keys, entries)
  end

  def compound_batch_mutate(store, redis_key, compound_keys, entries) when is_map(store) do
    case store do
      %{compound_batch_mutate: mutate_fun} when is_function(mutate_fun, 3) ->
        mutate_fun.(redis_key, compound_keys, entries)

      _ ->
        compound_batch_mutate_fallback(store, redis_key, compound_keys, entries)
    end
  end

  defp compound_batch_mutate_fallback(store, redis_key, compound_keys, entries) do
    entry_keys = Enum.map(entries, &elem(&1, 0))
    affected_keys = Enum.uniq(compound_keys ++ entry_keys)

    with {:ok, snapshot} <- compound_mutation_snapshot(store, redis_key, affected_keys) do
      case compound_batch_delete(store, redis_key, compound_keys) |> mutation_write_result() do
        :ok ->
          case compound_batch_put(store, redis_key, entries) |> mutation_write_result() do
            :ok ->
              :ok

            {:error, _reason} = write_error ->
              rollback_compound_mutation(store, redis_key, entry_keys, snapshot, write_error)
          end

        {:error, _reason} = delete_error ->
          if sequential_batch_delete?(store) do
            restore_compound_snapshot(store, redis_key, snapshot, delete_error)
          else
            delete_error
          end
      end
    end
  end

  defp compound_mutation_snapshot(store, redis_key, affected_keys) do
    metas = compound_batch_get_meta(store, redis_key, affected_keys)

    cond do
      not is_list(metas) or length(metas) != length(affected_keys) ->
        ReadResult.failure(:invalid_compound_mutation_snapshot)

      failure = ReadResult.first_failure(metas) ->
        failure

      not Enum.all?(metas, &valid_compound_snapshot_meta?/1) ->
        ReadResult.failure(:invalid_compound_mutation_snapshot)

      true ->
        snapshot =
          affected_keys
          |> Enum.zip(metas)
          |> Enum.flat_map(fn
            {_compound_key, nil} -> []
            {compound_key, {value, expire_at_ms}} -> [{compound_key, value, expire_at_ms}]
          end)

        {:ok, snapshot}
    end
  end

  defp valid_compound_snapshot_meta?(nil), do: true

  defp valid_compound_snapshot_meta?({value, expire_at_ms}),
    do: is_binary(value) and is_integer(expire_at_ms) and expire_at_ms >= 0

  defp valid_compound_snapshot_meta?(_meta), do: false

  defp sequential_batch_delete?(%FerricStore.Instance{}), do: false

  defp sequential_batch_delete?(store) when is_map(store) do
    case store do
      %{compound_batch_delete: fun} when is_function(fun, 2) -> false
      _fallback -> true
    end
  end

  defp restore_compound_snapshot(store, redis_key, snapshot, write_error) do
    case compound_batch_put(store, redis_key, snapshot) |> mutation_write_result() do
      :ok -> write_error
      restore -> {:error, {:compound_batch_mutate_rollback_failed, write_error, :ok, restore}}
    end
  end

  defp rollback_compound_mutation(store, redis_key, entry_keys, snapshot, write_error) do
    cleanup = compound_batch_delete(store, redis_key, entry_keys) |> mutation_write_result()
    restore = compound_batch_put(store, redis_key, snapshot) |> mutation_write_result()

    case {cleanup, restore} do
      {:ok, :ok} -> write_error
      _ -> {:error, {:compound_batch_mutate_rollback_failed, write_error, cleanup, restore}}
    end
  end

  defp mutation_write_result(:ok), do: :ok
  defp mutation_write_result(true), do: :ok
  defp mutation_write_result({:error, _reason} = error), do: error
  defp mutation_write_result(other), do: {:error, other}

  @spec compound_scan(store(), binary(), binary()) ::
          [{binary(), binary()}] | ReadResult.failure()
  def compound_scan(%FerricStore.Instance{} = ctx, redis_key, prefix),
    do: Router.compound_scan(ctx, redis_key, prefix)

  def compound_scan(%LocalTxStore{} = tx, redis_key, prefix) do
    if LocalRead.local?(tx, redis_key) do
      shard_data_path = LocalRead.promoted_path(tx, redis_key) || tx.shard_state.shard_data_path
      results = ShardETS.prefix_scan_entries(tx.shard_state, prefix, shard_data_path)

      ReadResult.map_success(results, fn values ->
        values
        |> LocalRead.merge_tx_pending_prefix(prefix)
        |> Enum.sort_by(fn {field, _value} -> field end)
      end)
    else
      idx = Router.shard_for(tx.instance_ctx, redis_key)

      case Router.safe_read_call(tx.instance_ctx, idx, {:compound_scan, redis_key, prefix}) do
        {:ok, results} -> results
        :unavailable -> ReadResult.failure(:shard_unavailable)
      end
    end
  end

  def compound_scan(store, redis_key, prefix) when is_map(store),
    do: store.compound_scan.(redis_key, prefix)

  @spec compound_scan_slice(
          store(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: [{binary(), binary()}] | ReadResult.failure()
  def compound_scan_slice(store, redis_key, prefix, start, count, total) when is_map(store) do
    case store do
      %{compound_scan_slice: fun} when is_function(fun, 5) ->
        fun.(redis_key, prefix, start, count, total)

      _fallback ->
        store
        |> compound_scan(redis_key, prefix)
        |> slice_scan_results(start, count)
    end
  end

  def compound_scan_slice(store, redis_key, prefix, start, count, _total) do
    store
    |> compound_scan(redis_key, prefix)
    |> slice_scan_results(start, count)
  end

  defp slice_scan_results(results, start, count) do
    ReadResult.map_success(results, fn pairs ->
      pairs
      |> Enum.sort_by(fn {member, _value} -> member end)
      |> Enum.slice(start, count)
    end)
  end

  @spec compound_scan_page(
          store(),
          binary(),
          binary(),
          0 | {:after, binary()},
          pos_integer(),
          binary() | nil,
          boolean()
        ) ::
          {:ok, {0 | {:after, binary()}, [{binary(), binary() | nil}]}} | ReadResult.failure()
  def compound_scan_page(
        %FerricStore.Instance{} = ctx,
        redis_key,
        prefix,
        cursor,
        count,
        match_pattern,
        fields_only
      ),
      do:
        Router.compound_scan_page(
          ctx,
          redis_key,
          prefix,
          cursor,
          count,
          match_pattern,
          fields_only
        )

  def compound_scan_page(
        %LocalTxStore{} = tx,
        redis_key,
        prefix,
        cursor,
        count,
        match_pattern,
        fields_only
      ) do
    if LocalRead.local?(tx, redis_key) do
      local_compound_scan_page(
        tx,
        redis_key,
        prefix,
        cursor,
        count,
        match_pattern,
        fields_only
      )
    else
      Router.compound_scan_page(
        tx.instance_ctx,
        redis_key,
        prefix,
        cursor,
        count,
        match_pattern,
        fields_only
      )
    end
  end

  def compound_scan_page(
        store,
        redis_key,
        prefix,
        cursor,
        count,
        match_pattern,
        fields_only
      )
      when is_map(store) do
    case store do
      %{compound_scan_page: fun} when is_function(fun, 6) ->
        fun.(redis_key, prefix, cursor, count, match_pattern, fields_only)

      _fallback ->
        ReadResult.failure(:compound_scan_page_unavailable)
    end
  end

  defp local_compound_scan_page(
         tx,
         redis_key,
         prefix,
         cursor,
         count,
         match_pattern,
         fields_only
       ) do
    index =
      Map.get(tx.shard_state, :compound_member_index) ||
        Map.get(tx.shard_state, :compound_member_index_name)

    case CompoundMemberIndex.scan_page(
           index,
           tx.shard_state,
           prefix,
           cursor,
           count,
           match_pattern
         ) do
      {:ok, {next_cursor, members}} when fields_only ->
        {:ok, {next_cursor, Enum.map(members, &{&1, nil})}}

      {:ok, {next_cursor, members}} ->
        compound_keys = Enum.map(members, &(prefix <> &1))
        values = compound_batch_get(tx, redis_key, compound_keys)

        cond do
          length(values) != length(members) ->
            ReadResult.failure(:invalid_compound_scan_page_reply)

          failure = ReadResult.first_failure(values) ->
            failure

          true ->
            pairs =
              members
              |> Enum.zip(values)
              |> Enum.reject(fn {_member, value} -> is_nil(value) end)

            {:ok, {next_cursor, pairs}}
        end

      {:error, reason} ->
        ReadResult.failure({:compound_scan_page_failed, reason})

      :unavailable ->
        ReadResult.failure(:compound_member_index_unavailable)
    end
  end

  @spec compound_fields(store(), binary(), binary()) :: [binary()] | ReadResult.failure()
  def compound_fields(%FerricStore.Instance{} = ctx, redis_key, prefix),
    do: Router.compound_fields(ctx, redis_key, prefix)

  def compound_fields(store, redis_key, prefix) do
    store
    |> compound_scan(redis_key, prefix)
    |> ReadResult.map_success(&Enum.map(&1, fn {field, _value} -> field end))
  end

  @spec compound_count(store(), binary(), binary()) :: non_neg_integer() | ReadResult.failure()
  def compound_count(%FerricStore.Instance{} = ctx, redis_key, prefix),
    do: Router.compound_count(ctx, redis_key, prefix)

  def compound_count(%LocalTxStore{} = tx, redis_key, prefix) do
    if LocalRead.local?(tx, redis_key) do
      ShardETS.prefix_count_entries(tx.shard_state, prefix)
    else
      idx = Router.shard_for(tx.instance_ctx, redis_key)

      case Router.safe_read_call(tx.instance_ctx, idx, {:compound_count, redis_key, prefix}) do
        {:ok, count} -> count
        :unavailable -> ReadResult.failure(:shard_unavailable)
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
      ShardETS.prefix_each_key(tx.shard_state.keydir, prefix, fn key ->
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
