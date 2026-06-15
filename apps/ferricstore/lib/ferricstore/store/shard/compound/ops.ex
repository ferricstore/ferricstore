defmodule Ferricstore.Store.Shard.Compound.Ops do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.{CompoundCommand, Promotion}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.ZSetIndex
  alias Ferricstore.Store.Shard.Compound.Promoted

  require Logger

  @record_header_size 26

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

  @spec handle_compound_scan(binary(), binary(), map()) :: {:reply, [{binary(), binary()}], map()}
  @doc false
  def handle_compound_scan(redis_key, prefix, state) do
    case Promoted.promoted_store(state, redis_key) do
      nil ->
        state =
          if ShardETS.prefix_has_pending_cold?(state.keydir, prefix) do
            ShardFlush.flush_pending_for_read(state)
          else
            state
          end

        results = ShardETS.prefix_scan_entries(state, prefix, state.shard_data_path)
        {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}

      dedicated_path ->
        results = ShardETS.prefix_scan_entries(state, prefix, dedicated_path)
        {:reply, Enum.sort_by(results, fn {field, _} -> field end), state}
    end
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

  @spec handle_compound_count(binary(), binary(), map()) :: {:reply, non_neg_integer(), map()}
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
    state = ensure_zset_score_index(state, redis_key)

    {:reply,
     {:ok, ZSetIndex.range(state.zset_score_index, redis_key, min_bound, max_bound, reverse?)},
     state}
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
    state = ensure_zset_score_index(state, redis_key)

    {:reply,
     {:ok,
      ZSetIndex.range_slice(
        state.zset_score_index,
        redis_key,
        min_bound,
        max_bound,
        reverse?,
        offset,
        count
      )}, state}
  end

  @spec handle_zset_score_count(binary(), term(), term(), map()) ::
          {:reply, {:ok, non_neg_integer()}, map()}
  @doc false
  def handle_zset_score_count(redis_key, min_bound, max_bound, state) do
    state = ensure_zset_score_index(state, redis_key)

    {:reply,
     {:ok,
      ZSetIndex.count(
        state.zset_score_index,
        state.zset_score_lookup,
        redis_key,
        min_bound,
        max_bound
      )}, state}
  end

  @spec handle_zset_score_count_many([{binary(), term(), term()}], map()) ::
          {:reply, {:ok, [non_neg_integer()]}, map()}
  @doc false
  def handle_zset_score_count_many(queries, state) when is_list(queries) do
    {counts, state} =
      Enum.map_reduce(queries, state, fn {redis_key, min_bound, max_bound}, acc_state ->
        acc_state = ensure_zset_score_index(acc_state, redis_key)

        count =
          ZSetIndex.count(
            acc_state.zset_score_index,
            acc_state.zset_score_lookup,
            redis_key,
            min_bound,
            max_bound
          )

        {count, acc_state}
      end)

    {:reply, {:ok, counts}, state}
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
    state = ensure_zset_score_index(state, redis_key)

    {:reply,
     {:ok,
      ZSetIndex.rank_range(state.zset_score_index, redis_key, start_idx, stop_idx, reverse?)},
     state}
  end

  @spec handle_zset_member_rank(binary(), binary(), boolean(), map()) ::
          {:reply, {:ok, non_neg_integer() | nil}, map()}
  @doc false
  def handle_zset_member_rank(redis_key, member, reverse?, state) do
    state = ensure_zset_score_index(state, redis_key)

    {:reply,
     {:ok,
      ZSetIndex.member_rank(
        state.zset_score_index,
        state.zset_score_lookup,
        redis_key,
        member,
        reverse?
      )}, state}
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

  defp handle_compound_put_raft(redis_key, compound_key, value, expire_at_ms, state) do
    tracked_state =
      case Promoted.promoted_store_for_compound(state, redis_key, compound_key) do
        nil ->
          state

        _dedicated_path ->
          Promoted.track_promoted_dead_bytes(
            state,
            redis_key,
            compound_key,
            promoted_record_size(compound_key, value)
          )
      end

    result =
      Ferricstore.Raft.Batcher.write(
        tracked_state.index,
        CompoundCommand.put(compound_key, value, expire_at_ms)
      )

    new_version = tracked_state.write_version + 1

    case result do
      :ok ->
        new_state = %{tracked_state | write_version: new_version}

        new_state =
          case Promoted.promoted_store_for_compound(new_state, redis_key, compound_key) do
            nil -> Promoted.maybe_promote(new_state, redis_key, compound_key)
            _dedicated_path -> Promoted.bump_promoted_writes(new_state, redis_key)
          end

        new_state =
          new_state
          |> compound_member_index_put(compound_key)
          |> ZSetIndex.apply_put(redis_key, compound_key, value)

        {:reply, :ok, new_state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_batch_put_raft(redis_key, entries, state) do
    tracked_state =
      Enum.reduce(entries, state, fn {compound_key, value, _expire_at_ms}, acc ->
        case Promoted.promoted_store_for_compound(acc, redis_key, compound_key) do
          nil ->
            acc

          _dedicated_path ->
            Promoted.track_promoted_dead_bytes(
              acc,
              redis_key,
              compound_key,
              promoted_record_size(compound_key, value)
            )
        end
      end)

    result =
      Ferricstore.Raft.Batcher.write(
        tracked_state.index,
        CompoundCommand.batch_put(redis_key, entries)
      )

    new_version = tracked_state.write_version + 1

    case CompoundCommand.normalize_batch_reply(result) do
      :ok ->
        new_state = %{tracked_state | write_version: new_version}

        new_state =
          case List.last(entries) do
            {compound_key, _value, _expire_at_ms} ->
              case Promoted.promoted_store_for_compound(new_state, redis_key, compound_key) do
                nil -> Promoted.maybe_promote(new_state, redis_key, compound_key)
                _dedicated_path -> Promoted.bump_promoted_writes(new_state, redis_key)
              end

            nil ->
              new_state
          end

        new_state =
          new_state
          |> compound_member_index_puts(entries)
          |> ZSetIndex.apply_puts(redis_key, entries)

        {:reply, :ok, new_state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp promoted_record_size(compound_key, value) when is_binary(value) do
    @record_header_size + byte_size(compound_key) + byte_size(value)
  end

  defp handle_compound_put_direct(redis_key, compound_key, value, expire_at_ms, state) do
    case Promoted.promoted_store_for_compound(state, redis_key, compound_key) do
      nil ->
        true = ShardETS.ets_insert(state, compound_key, value, expire_at_ms)
        new_pending = [{compound_key, value, expire_at_ms} | state.pending]
        new_version = state.write_version + 1
        new_state = %{state | pending: new_pending, write_version: new_version}

        new_state =
          if state.flush_in_flight == nil,
            do: ShardFlush.flush_pending(new_state),
            else: new_state

        new_state = Promoted.maybe_promote(new_state, redis_key, compound_key)

        new_state =
          new_state
          |> compound_member_index_put(compound_key)
          |> ZSetIndex.apply_put(redis_key, compound_key, value)

        {:reply, :ok, new_state}

      dedicated_path ->
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
              |> compound_member_index_put(compound_key)
              |> ZSetIndex.apply_put(redis_key, compound_key, value)

            {:reply, :ok, new_state}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted write failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp handle_compound_batch_put_direct(redis_key, entries, state) do
    entries
    |> Enum.chunk_by(fn {compound_key, _value, _expire_at_ms} ->
      compound_io_target(state, redis_key, compound_key)
    end)
    |> Enum.reduce_while({:reply, :ok, state}, fn group, {:reply, :ok, acc_state} ->
      {compound_key, _value, _expire_at_ms} = hd(group)
      target = compound_io_target(acc_state, redis_key, compound_key)

      case put_compound_key_group_direct(redis_key, group, target, acc_state) do
        {:reply, :ok, new_state} -> {:cont, {:reply, :ok, new_state}}
        {:reply, {:error, _} = err, new_state} -> {:halt, {:reply, err, new_state}}
        {:reply, other, new_state} -> {:halt, {:reply, other, new_state}}
      end
    end)
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
        write_version: state.write_version + length(entries)
    }

    new_state =
      if state.flush_in_flight == nil,
        do: ShardFlush.flush_pending(new_state),
        else: new_state

    {last_compound_key, _value, _expire_at_ms} = List.last(entries)

    new_state =
      new_state
      |> Promoted.maybe_promote(redis_key, last_compound_key)
      |> compound_member_index_puts(entries)
      |> ZSetIndex.apply_puts(redis_key, entries)

    {:reply, :ok, new_state}
  end

  defp put_compound_key_group_direct(
         redis_key,
         entries,
         {:promoted, dedicated_path},
         state
       ) do
    case Promoted.promoted_write_batch_values(state, dedicated_path, entries) do
      {:ok, locations} ->
        new_state =
          entries
          |> Enum.zip(locations)
          |> Enum.reduce(state, fn
            {{compound_key, value, expire_at_ms}, {fid, offset, value_size, record_size}}, acc ->
              acc = Promoted.track_promoted_dead_bytes(acc, redis_key, compound_key, record_size)

              ShardETS.ets_insert_with_location(
                acc,
                compound_key,
                value,
                expire_at_ms,
                fid,
                offset,
                value_size
              )

              Promoted.bump_promoted_writes(acc, redis_key)
          end)
          |> compound_member_index_puts(entries)
          |> ZSetIndex.apply_puts(redis_key, entries)

        {:reply, :ok, new_state}

      {:error, reason} ->
        Logger.error("Shard #{state.index}: promoted batch write failed: #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  end

  defp handle_compound_delete_raft(redis_key, compound_key, state) do
    tracked_state =
      if Promoted.promoted_store_for_compound(state, redis_key, compound_key) do
        Promoted.track_promoted_delete_bytes(state, redis_key, compound_key)
      else
        state
      end

    result =
      Ferricstore.Raft.Batcher.write(tracked_state.index, CompoundCommand.delete(compound_key))

    new_version = tracked_state.write_version + 1

    case result do
      :ok ->
        new_state =
          if Promoted.promoted_store_for_compound(tracked_state, redis_key, compound_key) do
            Promoted.bump_promoted_writes(tracked_state, redis_key)
          else
            tracked_state
          end

        new_state =
          new_state
          |> compound_member_index_delete(compound_key)
          |> ZSetIndex.apply_delete(redis_key, compound_key)

        {:reply, :ok, %{new_state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_batch_delete_raft(redis_key, compound_keys, state) do
    tracked_state =
      Enum.reduce(compound_keys, state, fn compound_key, acc ->
        if Promoted.promoted_store_for_compound(acc, redis_key, compound_key) do
          Promoted.track_promoted_delete_bytes(acc, redis_key, compound_key)
        else
          acc
        end
      end)

    result =
      Ferricstore.Raft.Batcher.write(
        tracked_state.index,
        CompoundCommand.batch_delete(redis_key, compound_keys)
      )

    new_version = tracked_state.write_version + 1

    case CompoundCommand.normalize_batch_reply(result) do
      :ok ->
        new_state =
          Enum.reduce(compound_keys, tracked_state, fn compound_key, acc ->
            acc =
              if Promoted.promoted_store_for_compound(acc, redis_key, compound_key) do
                Promoted.bump_promoted_writes(acc, redis_key)
              else
                acc
              end

            acc
            |> compound_member_index_delete(compound_key)
            |> ZSetIndex.apply_delete(redis_key, compound_key)
          end)

        {:reply, :ok, %{new_state | write_version: new_version}}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_delete_direct(redis_key, compound_key, state) do
    case Promoted.promoted_store_for_compound(state, redis_key, compound_key) do
      nil ->
        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)
        state = ShardFlush.track_delete_dead_bytes(state, compound_key)

        case NIF.v2_append_tombstone(state.active_file_path, compound_key) do
          {:ok, _} ->
            ShardETS.ets_delete_key(state, compound_key)

            new_pending =
              case state.pending do
                [] -> []
                pending -> Enum.reject(pending, fn {k, _, _} -> k == compound_key end)
              end

            new_version = state.write_version + 1

            new_state =
              state
              |> Map.merge(%{pending: new_pending, write_version: new_version})
              |> compound_member_index_delete(compound_key)
              |> ZSetIndex.apply_delete(redis_key, compound_key)

            {:reply, :ok, new_state}

          {:error, reason} ->
            Logger.error(
              "Shard #{state.index}: tombstone write failed for compound_delete: #{inspect(reason)}"
            )

            {:reply, {:error, reason}, state}
        end

      dedicated_path ->
        state = Promoted.track_promoted_delete_bytes(state, redis_key, compound_key)

        case Promoted.promoted_tombstone(dedicated_path, compound_key) do
          {:ok, _} ->
            ShardETS.ets_delete_key(state, compound_key)

            new_state =
              state
              |> Promoted.bump_promoted_writes(redis_key)
              |> compound_member_index_delete(compound_key)
              |> ZSetIndex.apply_delete(redis_key, compound_key)

            {:reply, :ok, new_state}

          {:error, reason} ->
            Logger.error("Shard #{state.index}: promoted tombstone failed: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end
    end
  end

  defp handle_compound_batch_delete_direct(redis_key, compound_keys, state) do
    compound_keys
    |> Enum.chunk_by(&compound_delete_target(state, redis_key, &1))
    |> Enum.reduce_while({:reply, :ok, state}, fn keys, {:reply, :ok, acc_state} ->
      target = compound_delete_target(acc_state, redis_key, hd(keys))

      case delete_compound_key_group_direct(redis_key, keys, target, acc_state) do
        {:reply, :ok, new_state} -> {:cont, {:reply, :ok, new_state}}
        {:reply, {:error, _} = err, new_state} -> {:halt, {:reply, err, new_state}}
        {:reply, other, new_state} -> {:halt, {:reply, other, new_state}}
      end
    end)
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
          |> Enum.reduce(%{new_state | pending: new_pending}, fn compound_key, acc ->
            acc
            |> compound_member_index_delete(compound_key)
            |> ZSetIndex.apply_delete(redis_key, compound_key)
          end)
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
    state =
      Enum.reduce(compound_keys, state, fn compound_key, acc ->
        Promoted.track_promoted_delete_bytes(acc, redis_key, compound_key)
      end)

    case Promoted.promoted_tombstone_batch(dedicated_path, compound_keys) do
      {:ok, _locations} ->
        Enum.each(compound_keys, fn compound_key ->
          ShardETS.ets_delete_key(state, compound_key)
        end)

        new_state =
          Enum.reduce(compound_keys, state, fn compound_key, acc ->
            acc
            |> Promoted.bump_promoted_writes(redis_key)
            |> compound_member_index_delete(compound_key)
            |> ZSetIndex.apply_delete(redis_key, compound_key)
          end)

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
        new_promoted = Map.delete(state.promoted_instances, redis_key)

        new_state =
          %{state | promoted_instances: new_promoted, write_version: new_version}
          |> compound_member_index_delete_prefix(prefix)
          |> ZSetIndex.clear_ready_key(redis_key)

        {:reply, :ok, new_state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  defp handle_compound_delete_prefix_direct(redis_key, prefix, state) do
    case Promoted.promoted_store(state, redis_key) do
      nil ->
        keys_to_delete = ShardETS.prefix_collect_keys(state.keydir, prefix)

        state = ShardFlush.await_in_flight(state)
        state = ShardFlush.flush_pending_sync(state)

        case Promoted.tombstone_and_delete_keys(state, keys_to_delete) do
          {:ok, new_state} ->
            new_state =
              %{new_state | write_version: new_state.write_version + 1}
              |> compound_member_index_delete_prefix(prefix)
              |> ZSetIndex.clear_ready_key(redis_key)

            {:reply, :ok, new_state}

          {{:error, reason}, new_state} ->
            Logger.error(
              "Shard #{state.index}: compound_delete_prefix tombstone failed: #{inspect(reason)}"
            )

            {:reply, {:error, reason}, new_state}
        end

      _dedicated ->
        keys_to_delete = ShardETS.prefix_collect_keys(state.keydir, prefix)

        Enum.each(keys_to_delete, fn key -> ShardETS.ets_delete_key(state, key) end)

        Promotion.cleanup_promoted!(
          redis_key,
          state.shard_data_path,
          state.keydir,
          state.data_dir,
          state.index,
          state.instance_ctx
        )

        new_promoted = Map.delete(state.promoted_instances, redis_key)

        new_state =
          %{state | promoted_instances: new_promoted, write_version: state.write_version + 1}
          |> compound_member_index_delete_prefix(prefix)
          |> ZSetIndex.clear_ready_key(redis_key)

        {:reply, :ok, new_state}
    end
  end

  # -------------------------------------------------------------------
  # Promotion helpers
  # -------------------------------------------------------------------

  defp ensure_zset_score_index(state, redis_key) do
    prefix = Ferricstore.Store.CompoundKey.zset_prefix(redis_key)
    data_path = Promoted.promoted_store(state, redis_key) || state.shard_data_path
    ZSetIndex.ensure(state, redis_key, prefix, data_path)
  end

  defp compound_member_index_put(state, compound_key) do
    CompoundMemberIndex.put(Map.get(state, :compound_member_index), compound_key)
    state
  end

  defp compound_member_index_puts(state, entries) do
    Enum.each(entries, fn {compound_key, _value, _expire_at_ms} ->
      CompoundMemberIndex.put(Map.get(state, :compound_member_index), compound_key)
    end)

    state
  end

  defp compound_member_index_delete(state, compound_key) do
    CompoundMemberIndex.delete(Map.get(state, :compound_member_index), compound_key)
    state
  end

  defp compound_member_index_delete_prefix(state, prefix) do
    CompoundMemberIndex.delete_prefix(Map.get(state, :compound_member_index), prefix)
    state
  end
end
