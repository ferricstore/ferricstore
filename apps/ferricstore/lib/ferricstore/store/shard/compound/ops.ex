defmodule Ferricstore.Store.Shard.Compound.Ops do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{CompoundCommand, Promotion, ReadResult}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ZSetIndex
  alias Ferricstore.Store.Shard.Compound.{Promoted, Read}

  require Logger

  @spec handle_compound_put(binary(), binary(), binary(), non_neg_integer(), map()) ::
          {:reply, term(), map()}
  @doc false
  def handle_compound_put(redis_key, compound_key, value, expire_at_ms, state) do
    if state.raft? do
      handle_compound_put_raft(redis_key, compound_key, value, expire_at_ms, state)
    else
      handle_compound_put_direct(redis_key, compound_key, value, expire_at_ms, state)
    end
  end

  @spec handle_compound_batch_put(
          binary(),
          [{binary(), binary(), non_neg_integer()}],
          map()
        ) :: {:reply, term(), map()}
  @doc false
  def handle_compound_batch_put(_redis_key, [], state), do: {:reply, :ok, state}

  def handle_compound_batch_put(redis_key, entries, state) do
    if state.raft? do
      handle_compound_batch_put_raft(redis_key, entries, state)
    else
      handle_compound_batch_put_direct(redis_key, entries, state)
    end
  end

  @spec handle_compound_delete(binary(), binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_delete(redis_key, compound_key, state) do
    if state.raft? do
      handle_compound_delete_raft(redis_key, compound_key, state)
    else
      handle_compound_delete_direct(redis_key, compound_key, state)
    end
  end

  @spec handle_compound_batch_delete(binary(), [binary()], map()) :: {:reply, term(), map()}
  @doc false
  def handle_compound_batch_delete(_redis_key, [], state), do: {:reply, :ok, state}

  def handle_compound_batch_delete(redis_key, compound_keys, state) do
    if state.raft? do
      handle_compound_batch_delete_raft(redis_key, compound_keys, state)
    else
      handle_compound_batch_delete_direct(redis_key, compound_keys, state)
    end
  end

  @spec handle_compound_scan(binary(), binary(), map()) ::
          {:reply, [{binary(), binary()}] | ReadResult.failure(), map()}
  @doc false
  def handle_compound_scan(redis_key, prefix, state) do
    case Promoted.promoted_store(state, redis_key) do
      nil ->
        state =
          if ShardETS.prefix_has_pending_cold?(state.keydir, prefix) do
            state
            |> ShardFlush.await_in_flight()
            |> ShardFlush.flush_pending_sync()
          else
            state
          end

        results = ShardETS.prefix_scan_entries(state, prefix, state.shard_data_path)
        {:reply, sort_compound_scan_results(results), state}

      dedicated_path ->
        results = ShardETS.prefix_scan_entries(state, prefix, dedicated_path)
        {:reply, sort_compound_scan_results(results), state}
    end
  end

  @spec handle_compound_scan_bounded(binary(), binary(), map(), map()) ::
          {:reply, term(), map()}
  @doc false
  def handle_compound_scan_bounded(redis_key, prefix, limits, state) do
    case Promoted.promoted_store(state, redis_key) do
      nil ->
        state =
          if ShardETS.prefix_has_pending_cold?(state.keydir, prefix) do
            state
            |> ShardFlush.await_in_flight()
            |> ShardFlush.flush_pending_sync()
          else
            state
          end

        results =
          ShardETS.prefix_scan_entries_bounded(
            state,
            prefix,
            state.shard_data_path,
            limits
          )

        {:reply, results, state}

      dedicated_path ->
        results = ShardETS.prefix_scan_entries_bounded(state, prefix, dedicated_path, limits)
        {:reply, results, state}
    end
  end

  @spec handle_compound_scan_page(
          binary(),
          binary(),
          0 | {:after, binary()},
          pos_integer(),
          binary() | nil,
          boolean(),
          map()
        ) :: {:reply, term(), map()}
  @doc false
  def handle_compound_scan_page(
        redis_key,
        prefix,
        cursor,
        count,
        match_pattern,
        fields_only,
        state
      ) do
    state =
      if ShardETS.prefix_has_pending_cold?(state.keydir, prefix) do
        state
        |> ShardFlush.await_in_flight()
        |> ShardFlush.flush_pending_sync()
      else
        state
      end

    index = Map.get(state, :compound_member_index)

    page =
      if CompoundMemberIndex.supports_prefix?(prefix) and CompoundMemberIndex.ready?(index) do
        CompoundMemberIndex.scan_page(
          index,
          state,
          prefix,
          cursor,
          count,
          match_pattern
        )
      else
        :unavailable
      end

    case page do
      {:ok, {next_cursor, members}} ->
        compound_scan_page_values(
          redis_key,
          prefix,
          next_cursor,
          members,
          fields_only,
          state
        )

      {:error, reason} ->
        {:reply, ReadResult.failure({:compound_scan_page_failed, reason}), state}

      :unavailable ->
        {:reply, ReadResult.failure(:compound_member_index_unavailable), state}
    end
  end

  defp compound_scan_page_values(
         _redis_key,
         _prefix,
         next_cursor,
         members,
         true,
         state
       ) do
    pairs = Enum.map(members, &{&1, nil})
    {:reply, {:ok, {next_cursor, pairs}}, state}
  end

  defp compound_scan_page_values(
         redis_key,
         prefix,
         next_cursor,
         members,
         false,
         state
       ) do
    compound_keys = Enum.map(members, &(prefix <> &1))
    {:reply, values, state} = Read.handle_compound_batch_get(redis_key, compound_keys, state)

    result =
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

    {:reply, result, state}
  end

  @spec handle_compound_fields(binary(), binary(), map()) :: {:reply, [binary()], map()}
  @doc false
  def handle_compound_fields(redis_key, prefix, state) do
    case Promoted.promoted_store(state, redis_key) do
      nil ->
        {:reply, Enum.sort(ShardETS.prefix_scan_fields(state, prefix)), state}

      _dedicated_path ->
        {:reply, Enum.sort(ShardETS.prefix_scan_fields(state, prefix)), state}
    end
  end

  @spec handle_compound_count(binary(), binary(), map()) ::
          {:reply, non_neg_integer() | ReadResult.failure(), map()}
  @doc false
  def handle_compound_count(redis_key, prefix, state) do
    case Promoted.promoted_store(state, redis_key) do
      nil ->
        {:reply, ShardETS.prefix_count_entries(state, prefix), state}

      _dedicated_path ->
        {:reply, ShardETS.prefix_count_entries(state, prefix), state}
    end
  end

  @spec handle_zset_score_range(binary(), term(), term(), boolean(), map()) ::
          {:reply, {:ok, [{binary(), float()}]}, map()}
  @doc false
  def handle_zset_score_range(redis_key, min_bound, max_bound, reverse?, state) do
    with_zset_score_index(state, redis_key, fn state ->
      ZSetIndex.range(state.zset_score_index, redis_key, min_bound, max_bound, reverse?)
    end)
  end

  @spec handle_zset_score_range_slice(
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all,
          map()
        ) ::
          {:reply, {:ok, [{binary(), float()}]}, map()}
  @doc false
  def handle_zset_score_range_slice(
        redis_key,
        min_bound,
        max_bound,
        reverse?,
        offset,
        count,
        state
      ) do
    with_zset_score_index(state, redis_key, fn state ->
      ZSetIndex.range_slice(
        state.zset_score_index,
        redis_key,
        min_bound,
        max_bound,
        reverse?,
        offset,
        count
      )
    end)
  end

  @spec handle_zset_score_count(binary(), term(), term(), map()) ::
          {:reply, {:ok, non_neg_integer()}, map()}
  @doc false
  def handle_zset_score_count(redis_key, min_bound, max_bound, state) do
    with_zset_score_index(state, redis_key, fn state ->
      ZSetIndex.count(
        state.zset_score_index,
        state.zset_score_lookup,
        redis_key,
        min_bound,
        max_bound
      )
    end)
  end

  @spec handle_zset_score_count_many([{binary(), term(), term()}], map()) ::
          {:reply, {:ok, [non_neg_integer()]}, map()}
  @doc false
  def handle_zset_score_count_many(queries, state) when is_list(queries) do
    result =
      Enum.reduce_while(queries, {:ok, [], state}, fn
        {redis_key, min_bound, max_bound}, {:ok, counts, acc_state} ->
          case ensure_zset_score_index(acc_state, redis_key) do
            {:ok, next_state} ->
              count =
                ZSetIndex.count(
                  next_state.zset_score_index,
                  next_state.zset_score_lookup,
                  redis_key,
                  min_bound,
                  max_bound
                )

              {:cont, {:ok, [count | counts], next_state}}

            {:error, {:storage_read_failed, _reason}} = failure ->
              {:halt, {:read_failure, failure, acc_state}}
          end
      end)

    case result do
      {:ok, counts, next_state} -> {:reply, {:ok, Enum.reverse(counts)}, next_state}
      {:read_failure, failure, next_state} -> {:reply, failure, next_state}
    end
  end

  @spec handle_zset_score_count_all_many_no_build([binary()], map()) ::
          {:reply, {:ok, [non_neg_integer()]}, map()}
  @doc false
  def handle_zset_score_count_all_many_no_build(keys, state) when is_list(keys) do
    counts =
      Enum.map(keys, fn key ->
        ZSetIndex.count(state.zset_score_index, state.zset_score_lookup, key, :neg_inf, :inf)
      end)

    {:reply, {:ok, counts}, state}
  end

  @spec handle_zset_rank_range(binary(), non_neg_integer(), non_neg_integer(), boolean(), map()) ::
          {:reply, {:ok, [{binary(), float()}]}, map()}
  @doc false
  def handle_zset_rank_range(redis_key, start_idx, stop_idx, reverse?, state) do
    with_zset_score_index(state, redis_key, fn state ->
      ZSetIndex.rank_range(state.zset_score_index, redis_key, start_idx, stop_idx, reverse?)
    end)
  end

  @spec handle_zset_member_rank(binary(), binary(), boolean(), map()) ::
          {:reply, {:ok, non_neg_integer() | nil}, map()}
  @doc false
  def handle_zset_member_rank(redis_key, member, reverse?, state) do
    with_zset_score_index(state, redis_key, fn state ->
      ZSetIndex.member_rank(
        state.zset_score_index,
        state.zset_score_lookup,
        redis_key,
        member,
        reverse?
      )
    end)
  end

  @spec handle_compound_delete_prefix(binary(), binary(), map()) :: {:reply, :ok, map()}
  @doc false
  def handle_compound_delete_prefix(redis_key, prefix, state) do
    if state.raft? do
      handle_compound_delete_prefix_raft(redis_key, prefix, state)
    else
      handle_compound_delete_prefix_direct(redis_key, prefix, state)
    end
  end

  # -------------------------------------------------------------------
  # Raft / direct write helpers
  # -------------------------------------------------------------------

  defp handle_compound_put_raft(_redis_key, compound_key, value, expire_at_ms, state) do
    result =
      Ferricstore.Raft.Batcher.write(
        state.index,
        CompoundCommand.put(compound_key, value, expire_at_ms)
      )

    new_version = state.write_version + 1

    case result do
      :ok ->
        {:reply, :ok, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_batch_put_raft(redis_key, entries, state) do
    result =
      Ferricstore.Raft.Batcher.write(
        state.index,
        CompoundCommand.batch_put(redis_key, entries)
      )

    new_version = state.write_version + 1

    case CompoundCommand.normalize_batch_reply(result, length(entries)) do
      :ok ->
        {:reply, :ok, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_put_direct(redis_key, compound_key, value, expire_at_ms, state) do
    case Promoted.promoted_store_for_compound(state, redis_key, compound_key) do
      nil ->
        true = ShardETS.ets_insert(state, compound_key, value, expire_at_ms)
        new_pending = [{compound_key, value, expire_at_ms} | state.pending]
        new_version = state.write_version + 1

        new_state = %{
          state
          | pending: new_pending,
            pending_count: Map.get(state, :pending_count, length(state.pending)) + 1,
            write_version: new_version
        }

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        new_state =
          new_state
          |> ZSetIndex.apply_put(redis_key, compound_key, value)
          |> Promoted.maybe_promote(redis_key, compound_key)

        {:reply, :ok, new_state}

      dedicated_path ->
        Promotion.await_compaction_latch(state, redis_key)

        case Promoted.promoted_write_value(
               state,
               dedicated_path,
               compound_key,
               value,
               expire_at_ms
             ) do
          {:ok, {fid, offset, value_size, record_size}} ->
            state =
              Promoted.track_promoted_dead_bytes(state, redis_key, compound_key, record_size)

            ShardETS.ets_insert_with_location(
              state,
              compound_key,
              value,
              expire_at_ms,
              fid,
              offset,
              value_size
            )

            new_state =
              state
              |> Promoted.bump_promoted_writes(redis_key)
              |> ZSetIndex.apply_put(redis_key, compound_key, value)
              |> Map.put(:write_version, state.write_version + 1)

            {:reply, :ok, new_state}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted write failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp handle_compound_batch_put_direct(redis_key, entries, state) do
    {first_key, _value, _expire_at_ms} = hd(entries)
    target = compound_io_target(state, redis_key, first_key)

    if Enum.all?(entries, fn {compound_key, _value, _expire_at_ms} ->
         compound_io_target(state, redis_key, compound_key) == target
       end) do
      put_compound_key_group_direct(redis_key, entries, target, state)
    else
      {:reply, {:error, :mixed_compound_batch_targets}, state}
    end
  end

  defp handle_compound_batch_delete_direct(redis_key, compound_keys, state) do
    target = compound_delete_target(state, redis_key, hd(compound_keys))

    if Enum.all?(compound_keys, fn compound_key ->
         compound_delete_target(state, redis_key, compound_key) == target
       end) do
      delete_compound_key_group_direct(redis_key, compound_keys, target, state)
    else
      {:reply, {:error, :mixed_compound_batch_targets}, state}
    end
  end

  defp compound_delete_target(state, redis_key, compound_key) do
    compound_io_target(state, redis_key, compound_key)
  end

  defp compound_io_target(state, redis_key, compound_key) do
    case Promoted.promoted_store_for_compound(state, redis_key, compound_key) do
      nil -> :shared
      dedicated_path -> {:promoted, dedicated_path}
    end
  end

  defp put_compound_key_group_direct(redis_key, entries, :shared, state) do
    Enum.each(entries, fn {compound_key, value, expire_at_ms} ->
      true = ShardETS.ets_insert(state, compound_key, value, expire_at_ms)
    end)

    new_pending =
      Enum.reduce(entries, state.pending, fn {compound_key, value, expire_at_ms}, pending ->
        [{compound_key, value, expire_at_ms} | pending]
      end)

    new_state = %{
      state
      | pending: new_pending,
        pending_count: Map.get(state, :pending_count, length(state.pending)) + length(entries),
        write_version: state.write_version + length(entries)
    }

    new_state =
      if state.flush_in_flight == nil,
        do: ShardFlush.flush_pending(new_state),
        else: new_state

    {last_compound_key, _value, _expire_at_ms} = List.last(entries)

    new_state =
      new_state
      |> ZSetIndex.apply_puts(redis_key, entries)
      |> Promoted.maybe_promote(redis_key, last_compound_key)

    {:reply, :ok, new_state}
  end

  defp put_compound_key_group_direct(
         redis_key,
         entries,
         {:promoted, dedicated_path},
         state
       ) do
    Promotion.await_compaction_latch(state, redis_key)

    case Promoted.promoted_write_batch_values(state, dedicated_path, entries) do
      {:ok, locations} ->
        new_state =
          entries
          |> Enum.zip(locations)
          |> Enum.reduce(state, fn
            {{compound_key, value, expire_at_ms}, {fid, offset, value_size, record_size}}, acc ->
              acc = Promoted.track_promoted_dead_bytes(acc, redis_key, compound_key, record_size)

              true =
                ShardETS.ets_insert_with_location(
                  acc,
                  compound_key,
                  value,
                  expire_at_ms,
                  fid,
                  offset,
                  value_size
                )

              acc
          end)
          |> Promoted.bump_promoted_writes(redis_key)
          |> ZSetIndex.apply_puts(redis_key, entries)
          |> Map.put(:write_version, state.write_version + length(entries))

        {:reply, :ok, new_state}

      {:error, reason} ->
        Logger.error("Shard #{state.index}: promoted batch write failed: #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  end

  defp handle_compound_delete_raft(_redis_key, compound_key, state) do
    result = Ferricstore.Raft.Batcher.write(state.index, CompoundCommand.delete(compound_key))
    new_version = state.write_version + 1

    case result do
      :ok ->
        {:reply, :ok, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_batch_delete_raft(redis_key, compound_keys, state) do
    result =
      Ferricstore.Raft.Batcher.write(
        state.index,
        CompoundCommand.batch_delete(redis_key, compound_keys)
      )

    new_version = state.write_version + 1

    case CompoundCommand.normalize_batch_reply(result, length(compound_keys)) do
      :ok ->
        {:reply, :ok, %{state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_delete_direct(redis_key, compound_key, state) do
    case Promoted.promoted_store_for_compound(state, redis_key, compound_key) do
      nil ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case NIF.v2_append_tombstone(state.active_file_path, compound_key) do
          {:ok, _} ->
            state = ShardFlush.track_delete_dead_bytes(state, compound_key)
            ShardETS.ets_delete_key(state, compound_key)

            new_pending =
              case state.pending do
                [] -> []
                pending -> Enum.reject(pending, fn {k, _, _} -> k == compound_key end)
              end

            new_version = state.write_version + 1

            new_state =
              state
              |> Map.merge(%{
                pending: new_pending,
                pending_count: length(new_pending),
                write_version: new_version
              })
              |> ZSetIndex.apply_delete(redis_key, compound_key)

            {:reply, :ok, new_state}

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: tombstone write failed for compound_delete: #{inspect(reason)}"
            )

            {:reply, {:error, reason}, state}
        end

      dedicated_path ->
        Promotion.await_compaction_latch(state, redis_key)

        case Promoted.promoted_tombstone(dedicated_path, compound_key) do
          {:ok, _} ->
            state = Promoted.track_promoted_delete_bytes(state, redis_key, compound_key)
            ShardETS.ets_delete_key(state, compound_key)

            new_state =
              state
              |> Promoted.bump_promoted_writes(redis_key)
              |> ZSetIndex.apply_delete(redis_key, compound_key)
              |> Map.put(:write_version, state.write_version + 1)

            {:reply, :ok, new_state}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted tombstone failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp delete_compound_key_group_direct(redis_key, compound_keys, :shared, state) do
    state = ShardFlush.await_in_flight(state)
    state = ShardFlush.flush_pending_sync(state)

    case Promoted.tombstone_and_delete_keys(state, compound_keys) do
      {:ok, new_state} ->
        compound_key_set = MapSet.new(compound_keys)

        new_pending =
          case new_state.pending do
            [] ->
              []

            pending ->
              Enum.reject(pending, fn {k, _, _} -> MapSet.member?(compound_key_set, k) end)
          end

        new_state =
          compound_keys
          |> Enum.reduce(
            %{new_state | pending: new_pending, pending_count: length(new_pending)},
            fn compound_key, acc ->
            ZSetIndex.apply_delete(acc, redis_key, compound_key)
            end
          )
          |> Map.update!(:write_version, &(&1 + length(compound_keys)))

        {:reply, :ok, new_state}

      {{:error, reason}, new_state} ->
        Logger.error("Shard #{state.index}: compound batch tombstone failed: #{inspect(reason)}")

        {:reply, {:error, reason}, new_state}
    end
  end

  defp delete_compound_key_group_direct(
         redis_key,
         compound_keys,
         {:promoted, dedicated_path},
       state
       ) do
    Promotion.await_compaction_latch(state, redis_key)

    case Promoted.promoted_tombstone_batch(dedicated_path, compound_keys) do
      {:ok, _locations} ->
        state =
          Enum.reduce(compound_keys, state, fn compound_key, acc ->
            Promoted.track_promoted_delete_bytes(acc, redis_key, compound_key)
          end)

        Enum.each(compound_keys, fn compound_key ->
          ShardETS.ets_delete_key(state, compound_key)
        end)

        new_state =
          Enum.reduce(compound_keys, state, fn compound_key, acc ->
            ZSetIndex.apply_delete(acc, redis_key, compound_key)
          end)
          |> Promoted.bump_promoted_writes(redis_key)
          |> Map.put(:write_version, state.write_version + length(compound_keys))

        {:reply, :ok, new_state}

      {:error, reason} ->
        Logger.error("Shard #{state.index}: promoted tombstone batch failed: #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  end

  defp handle_compound_delete_prefix_raft(redis_key, prefix, state) do
    result = Ferricstore.Raft.Batcher.write(state.index, CompoundCommand.delete_prefix(prefix))
    new_version = state.write_version + 1

    case result do
      :ok ->
        promoted_instances =
          case promoted_prefix_cleanup_target(state, redis_key, prefix) do
            nil -> state.promoted_instances
            {_type, _path} -> Map.delete(state.promoted_instances, redis_key)
          end

        new_state =
          %{
            state
            | promoted_instances: promoted_instances,
              write_version: new_version
          }
          |> compound_member_index_delete_prefix(prefix)
          |> ZSetIndex.clear_ready_key(redis_key)

        {:reply, :ok, new_state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_delete_prefix_direct(redis_key, prefix, state) do
    case CompoundMemberIndex.keys_for_prefix(Map.get(state, :compound_member_index), prefix) do
      {:ok, keys_to_delete} ->
        cleanup_target = promoted_prefix_cleanup_target(state, redis_key, prefix)

        target =
          case Promoted.promoted_store(state, redis_key) do
            nil -> :shared
            dedicated_path -> {:promoted, dedicated_path}
          end

        case delete_compound_key_group_direct(redis_key, keys_to_delete, target, state) do
          {:reply, :ok, new_state} ->
            new_state = cleanup_promoted_prefix!(new_state, redis_key, cleanup_target)

            new_state =
              %{new_state | write_version: new_state.write_version + 1}
              |> compound_member_index_delete_prefix(prefix)
              |> ZSetIndex.clear_ready_key(redis_key)

            {:reply, :ok, new_state}

          {:reply, {:error, _reason} = error, new_state} ->
            {:reply, error, new_state}
        end

      :unavailable ->
        {:reply, {:error, :compound_member_index_unavailable}, state}
    end
  end

  defp promoted_prefix_cleanup_target(state, redis_key, prefix) do
    with {type, ^prefix} <- Promoted.detect_compound_type(redis_key, prefix),
         dedicated_path when is_binary(dedicated_path) <-
           Promoted.promoted_store(state, redis_key) do
      {type, dedicated_path}
    else
      _not_an_exact_promoted_prefix -> nil
    end
  end

  defp cleanup_promoted_prefix!(state, _redis_key, nil), do: state

  defp cleanup_promoted_prefix!(state, redis_key, {type, dedicated_path}) do
    :ok =
      Promotion.cleanup_promoted!(
        redis_key,
        type,
        dedicated_path,
        state.shard_data_path,
        state.keydir,
        state.data_dir,
        state.index,
        state.instance_ctx
      )

    %{state | promoted_instances: Map.delete(state.promoted_instances, redis_key)}
  end

  # -------------------------------------------------------------------
  # Promotion helpers
  # -------------------------------------------------------------------

  defp ensure_zset_score_index(state, redis_key) do
    prefix = Ferricstore.Store.CompoundKey.zset_prefix(redis_key)
    data_path = Promoted.promoted_store(state, redis_key) || state.shard_data_path
    ZSetIndex.ensure(state, redis_key, prefix, data_path)
  end

  defp with_zset_score_index(state, redis_key, fun) do
    case ensure_zset_score_index(state, redis_key) do
      {:ok, next_state} -> {:reply, {:ok, fun.(next_state)}, next_state}
      {:error, {:storage_read_failed, _reason}} = failure -> {:reply, failure, state}
    end
  end

  defp sort_compound_scan_results(results) do
    ReadResult.map_success(results, &Enum.sort_by(&1, fn {field, _value} -> field end))
  end

  defp compound_member_index_delete_prefix(state, prefix) do
    CompoundMemberIndex.delete_prefix(Map.get(state, :compound_member_index), prefix)
    state
  end
end
