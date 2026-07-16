defmodule Ferricstore.Store.Router.Part11 do
  @moduledoc false

  # Extracted from Router: flow_index_count_all_many .. checked_lmove
  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.ErrorReasons
      alias Ferricstore.HLC
      alias Ferricstore.HyperLogLog, as: HLL
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Raft.ReplyAwaiter
      alias Ferricstore.Stats
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.CompoundCommand
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.ListOps
      alias Ferricstore.Store.ReadResult
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.Shard.ZSetIndex
      alias Ferricstore.Store.SlotMap
      alias Ferricstore.Store.TypeRegistry

      @spec flow_index_count_all_many(FerricStore.Instance.t(), [binary()]) ::
              {:ok, [non_neg_integer()]} | :unavailable
      def flow_index_count_all_many(_ctx, []), do: {:ok, []}

      def flow_index_count_all_many(ctx, [first_key | _] = keys) do
        result =
          if selected_waraft_ctx?(ctx) do
            direct_flow_index_count_all_many(ctx, keys)
          else
            idx = shard_for(ctx, first_key)

            if Enum.all?(keys, fn key -> shard_for(ctx, key) == idx end) do
              ctx
              |> safe_read_call(idx, {:flow_index_count_all_many, keys})
              |> unwrap_zset_index_reply()
            else
              flow_index_count_all_many_cross_shard(ctx, keys)
            end
          end

        validate_flow_index_count_batch(result, length(keys))
      end

      defp flow_index_count_all_many_cross_shard(ctx, keys) do
        Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
          case flow_index_count_all(ctx, key) do
            {:ok, count} when is_integer(count) and count >= 0 ->
              {:cont, {:ok, [count | acc]}}

            :unavailable ->
              {:halt, :unavailable}

            _other ->
              {:halt, :unavailable}
          end
        end)
        |> case do
          {:ok, counts} -> {:ok, Enum.reverse(counts)}
          :unavailable -> :unavailable
        end
      end

      @doc false
      @spec flow_earliest_due_score(
              FerricStore.Instance.t(),
              [binary()],
              [binary()],
              [binary()]
            ) :: {:ok, float() | nil} | :unavailable
      def flow_earliest_due_score(ctx, prefixes, needles, suffixes)
          when is_list(prefixes) and is_list(needles) and is_list(suffixes) do
        shard_count = max(1, Map.get(ctx, :shard_count, 1))

        0..(shard_count - 1)//1
        |> Enum.reduce_while({:ok, nil}, fn idx, {:ok, earliest} ->
          case flow_earliest_due_score(ctx, idx, prefixes, needles, suffixes) do
            {:ok, nil} ->
              {:cont, {:ok, earliest}}

            {:ok, score} when is_float(score) ->
              next = if is_float(earliest), do: min(earliest, score), else: score
              {:cont, {:ok, next}}

            _unavailable_or_invalid ->
              {:halt, :unavailable}
          end
        end)
      end

      def flow_earliest_due_score(_ctx, _prefixes, _needles, _suffixes), do: :unavailable

      @spec zset_rank_range(
              FerricStore.Instance.t(),
              binary(),
              non_neg_integer(),
              non_neg_integer(),
              boolean()
            ) ::
              {:ok, [{binary(), float()}]} | :unavailable
      def zset_rank_range(ctx, redis_key, start_idx, stop_idx, reverse?) do
        idx = shard_for(ctx, redis_key)

        if selected_waraft_ctx?(ctx) do
          case direct_zset_rank_range(ctx, idx, redis_key, start_idx, stop_idx, reverse?) do
            {:error, {:storage_read_failed, _reason}} = failure -> failure
            result -> {:ok, result}
          end
        else
          ctx
          |> safe_read_call(idx, {:zset_rank_range, redis_key, start_idx, stop_idx, reverse?})
          |> unwrap_zset_index_reply()
        end
      end

      @spec zset_member_rank(FerricStore.Instance.t(), binary(), binary(), boolean()) ::
              {:ok, non_neg_integer() | nil} | :unavailable
      def zset_member_rank(ctx, redis_key, member, reverse?) do
        idx = shard_for(ctx, redis_key)

        if selected_waraft_ctx?(ctx) do
          case direct_zset_member_rank(ctx, idx, redis_key, member, reverse?) do
            {:error, {:storage_read_failed, _reason}} = failure -> failure
            result -> {:ok, result}
          end
        else
          ctx
          |> safe_read_call(idx, {:zset_member_rank, redis_key, member, reverse?})
          |> unwrap_zset_index_reply()
        end
      end

      defp unwrap_zset_index_reply({:ok, {:ok, result}}), do: {:ok, result}
      defp unwrap_zset_index_reply({:ok, :unavailable}), do: :unavailable

      defp unwrap_zset_index_reply({:ok, {:error, {:storage_read_failed, _reason}} = failure}),
        do: failure

      defp unwrap_zset_index_reply(other), do: other

      defp direct_flow_index_score_range_slice(
             ctx,
             idx,
             key,
             min_bound,
             max_bound,
             reverse?,
             offset,
             count
           ) do
        direct_flow_index_read(ctx, idx, fn native ->
          NativeFlowIndex.range_slice(native, key, min_bound, max_bound, reverse?, offset, count)
        end)
      end

      defp direct_flow_index_rank_range(_ctx, _idx, _key, start_idx, stop_idx, _reverse?)
           when start_idx > stop_idx,
           do: {:ok, []}

      defp direct_flow_index_rank_range(ctx, idx, key, start_idx, stop_idx, reverse?) do
        direct_flow_index_read(ctx, idx, fn native ->
          NativeFlowIndex.rank_range(native, key, start_idx, stop_idx, reverse?)
        end)
      end

      defp direct_flow_index_rank_range_many(ctx, requests) do
        requests
        |> Enum.with_index()
        |> Enum.group_by(fn {{key, _start_idx, _stop_idx, _reverse?}, _index} ->
          shard_for(ctx, key)
        end)
        |> Enum.reduce_while({:ok, %{}}, fn {idx, indexed_requests}, {:ok, acc} ->
          shard_requests = Enum.map(indexed_requests, fn {request, _index} -> request end)

          case direct_flow_index_read(ctx, idx, fn native ->
                 Enum.map(shard_requests, fn {key, start_idx, stop_idx, reverse?} ->
                   NativeFlowIndex.rank_range(native, key, start_idx, stop_idx, reverse?)
                 end)
               end) do
            {:ok, results} when is_list(results) ->
              if valid_flow_index_rank_batch?(results, length(indexed_requests)) do
                indexed =
                  indexed_requests
                  |> Enum.zip(results)
                  |> Enum.reduce(acc, fn {{_request, original_index}, result}, next_acc ->
                    Map.put(next_acc, original_index, result)
                  end)

                {:cont, {:ok, indexed}}
              else
                {:halt, :unavailable}
              end

            :unavailable ->
              {:halt, :unavailable}

            _other ->
              {:halt, :unavailable}
          end
        end)
        |> case do
          {:ok, indexed} ->
            {:ok, Enum.map(0..(length(requests) - 1)//1, &Map.fetch!(indexed, &1))}

          :unavailable ->
            :unavailable
        end
      end

      defp direct_flow_index_count_all(ctx, idx, key) do
        direct_flow_index_read(ctx, idx, fn native ->
          NativeFlowIndex.count_all(native, key)
        end)
      end

      defp direct_flow_index_count_all_many(ctx, keys) do
        keys
        |> Enum.with_index()
        |> Enum.group_by(fn {key, _index} -> shard_for(ctx, key) end)
        |> Enum.reduce_while({:ok, %{}}, fn {idx, indexed_keys}, {:ok, acc} ->
          shard_keys = Enum.map(indexed_keys, fn {key, _index} -> key end)

          case direct_flow_index_read(ctx, idx, fn native ->
                 NativeFlowIndex.count_many(native, shard_keys)
               end) do
            {:ok, counts} when is_list(counts) ->
              if valid_flow_index_count_batch?(counts, length(indexed_keys)) do
                indexed =
                  indexed_keys
                  |> Enum.zip(counts)
                  |> Enum.reduce(acc, fn {{_key, original_index}, count}, next_acc ->
                    Map.put(next_acc, original_index, count)
                  end)

                {:cont, {:ok, indexed}}
              else
                {:halt, :unavailable}
              end

            :unavailable ->
              {:halt, :unavailable}

            _other ->
              {:halt, :unavailable}
          end
        end)
        |> case do
          {:ok, indexed} ->
            {:ok, Enum.map(0..(length(keys) - 1)//1, &Map.fetch!(indexed, &1))}

          :unavailable ->
            :unavailable
        end
      end

      defp flow_earliest_due_score(ctx, idx, prefixes, needles, suffixes) do
        if selected_waraft_ctx?(ctx) do
          direct_flow_index_read(ctx, idx, fn native ->
            NativeFlowIndex.earliest_due_score(native, prefixes, needles, suffixes)
          end)
        else
          ctx
          |> safe_read_call(idx, {:flow_earliest_due_score, prefixes, needles, suffixes})
          |> unwrap_zset_index_reply()
        end
      end

      defp direct_flow_index_read(ctx, idx, fun) do
        {flow_index, flow_lookup} = NativeFlowIndex.table_names(ctx.name, idx)

        case NativeFlowIndex.get(flow_index, flow_lookup) do
          nil -> :unavailable
          native -> {:ok, fun.(native)}
        end
      rescue
        ArgumentError -> :unavailable
      end

      defp validate_flow_index_count_batch({:ok, counts}, expected_count)
           when is_list(counts) do
        if valid_flow_index_count_batch?(counts, expected_count),
          do: {:ok, counts},
          else: :unavailable
      end

      defp validate_flow_index_count_batch(_result, _expected_count), do: :unavailable

      defp valid_flow_index_count_batch?(counts, expected_count) do
        length(counts) == expected_count and
          Enum.all?(counts, &(is_integer(&1) and &1 >= 0))
      end

      defp valid_flow_index_rank_batch?(results, expected_count) do
        length(results) == expected_count and
          Enum.all?(results, fn
            entries when is_list(entries) ->
              Enum.all?(entries, fn
                {member, score} when is_binary(member) and is_number(score) -> true
                _invalid -> false
              end)

            _invalid ->
              false
          end)
      end

      defp direct_zset_score_range(ctx, idx, redis_key, min_bound, max_bound, reverse?) do
        case direct_zset_index_read(ctx, idx, redis_key, fn index, _lookup ->
               ZSetIndex.range(index, redis_key, min_bound, max_bound, reverse?)
             end) do
          {:ok, members} ->
            members

          :unavailable ->
            ctx
            |> direct_zset_sorted_members(idx, redis_key, reverse?)
            |> ReadResult.map_success(fn members ->
              Enum.filter(members, fn {_member, score} ->
                zset_score_gte_bound?(score, min_bound) and
                  zset_score_lte_bound?(score, max_bound)
              end)
            end)
        end
      end

      defp direct_zset_score_count(ctx, idx, redis_key, min_bound, max_bound) do
        case direct_zset_index_read(ctx, idx, redis_key, fn index, lookup ->
               ZSetIndex.count(index, lookup, redis_key, min_bound, max_bound)
             end) do
          {:ok, count} ->
            count

          :unavailable ->
            ctx
            |> direct_zset_score_range(idx, redis_key, min_bound, max_bound, false)
            |> ReadResult.map_success(&length/1)
        end
      end

      defp direct_zset_rank_range(_ctx, _idx, _redis_key, start_idx, stop_idx, _reverse?)
           when start_idx > stop_idx,
           do: []

      defp direct_zset_rank_range(ctx, idx, redis_key, start_idx, stop_idx, reverse?) do
        case direct_zset_index_read(ctx, idx, redis_key, fn index, _lookup ->
               ZSetIndex.rank_range(index, redis_key, start_idx, stop_idx, reverse?)
             end) do
          {:ok, members} ->
            members

          :unavailable ->
            ctx
            |> direct_zset_sorted_members(idx, redis_key, reverse?)
            |> ReadResult.map_success(&Enum.slice(&1, start_idx..stop_idx))
        end
      end

      defp direct_zset_member_rank(ctx, idx, redis_key, member, reverse?) do
        case direct_zset_index_read(ctx, idx, redis_key, fn index, lookup ->
               ZSetIndex.member_rank(index, lookup, redis_key, member, reverse?)
             end) do
          {:ok, rank} ->
            rank

          :unavailable ->
            ctx
            |> direct_zset_sorted_members(idx, redis_key, reverse?)
            |> ReadResult.map_success(fn members ->
              Enum.find_index(members, fn {candidate, _score} -> candidate == member end)
            end)
        end
      end

      defp direct_zset_index_read(ctx, idx, redis_key, fun) do
        {index, lookup} = ZSetIndex.table_names(ctx.name, idx)

        if :ets.info(index) != :undefined and :ets.info(lookup) != :undefined and
             ZSetIndex.ready?(lookup, redis_key) do
          {:ok, fun.(index, lookup)}
        else
          :unavailable
        end
      rescue
        ArgumentError -> :unavailable
      end

      defp direct_zset_sorted_members(ctx, idx, redis_key, false) do
        ctx
        |> direct_zset_members(idx, redis_key)
        |> ReadResult.map_success(&Enum.sort_by(&1, fn {member, score} -> {score, member} end))
      end

      defp direct_zset_sorted_members(ctx, idx, redis_key, true) do
        ctx
        |> direct_zset_sorted_members(idx, redis_key, false)
        |> ReadResult.map_success(&Enum.reverse/1)
      end

      defp direct_zset_members(ctx, idx, redis_key) do
        idx
        |> direct_compound_scan(ctx, CompoundKey.zset_prefix(redis_key))
        |> ReadResult.map_success(fn entries ->
          Enum.flat_map(entries, fn {member, score_str} ->
            case Float.parse(score_str) do
              {score, ""} -> [{member, score}]
              _ -> []
            end
          end)
        end)
      end

      defp apply_zset_slice(_members, _offset, 0), do: []
      defp apply_zset_slice(members, 0, :all), do: members
      defp apply_zset_slice(members, offset, :all), do: Enum.drop(members, offset)

      defp apply_zset_slice(members, offset, count),
        do: members |> Enum.drop(offset) |> Enum.take(count)

      defp zset_score_gte_bound?(_score, :neg_inf), do: true
      defp zset_score_gte_bound?(_score, :inf), do: false
      defp zset_score_gte_bound?(score, {:exclusive, bound}), do: score > bound
      defp zset_score_gte_bound?(score, {:inclusive, bound}), do: score >= bound

      defp zset_score_lte_bound?(_score, :inf), do: true
      defp zset_score_lte_bound?(_score, :neg_inf), do: false
      defp zset_score_lte_bound?(score, {:exclusive, bound}), do: score < bound
      defp zset_score_lte_bound?(score, {:inclusive, bound}), do: score <= bound

      @spec compound_delete_prefix(FerricStore.Instance.t(), binary(), binary()) :: :ok
      def compound_delete_prefix(ctx, redis_key, prefix) do
        idx = shard_for(ctx, redis_key)

        if durable_raft_ctx?(ctx) do
          quorum_write(ctx, idx, CompoundCommand.delete_prefix(prefix))
        else
          safe_write_call(ctx, idx, {:compound_delete_prefix, redis_key, prefix})
        end
      end

      # -------------------------------------------------------------------
      # List operations
      # -------------------------------------------------------------------

      @spec list_op(FerricStore.Instance.t(), binary(), term()) :: term()
      def list_op(ctx, key, {:lmove, destination, from_dir, to_dir}) do
        source_idx = shard_for(ctx, key)

        if source_idx == shard_for(ctx, destination) do
          raft_write(ctx, source_idx, key, {:list_op_lmove, key, destination, from_dir, to_dir})
        else
          Ferricstore.CrossShardOp.execute(
            [{key, :read_write}, {destination, :write}],
            fn unified_store ->
              checked_lmove(key, destination, unified_store, from_dir, to_dir)
            end,
            instance: ctx
          )
        end
      end

      def list_op(ctx, key, operation) do
        idx = shard_for(ctx, key)

        if ListOps.read_operation?(operation) do
          if selected_waraft_ctx?(ctx) do
            with :ok <- TypeRegistry.command_check_type(key, :list, ctx),
                 do: ListOps.execute(key, ctx, operation)
          else
            case safe_read_call(ctx, idx, {:list_read, key, operation}) do
              {:ok, result} -> result
              :unavailable -> ReadResult.failure(:shard_unavailable) |> ReadResult.command_error()
            end
          end
        else
          raft_write(ctx, idx, key, {:list_op, key, operation})
        end
      end

      defp checked_lmove(source, destination, store, from_dir, to_dir) do
        with :ok <- TypeRegistry.check_type(source, :list, store) do
          case ListOps.read_meta(source, store) do
            nil ->
              nil

            {0, _, _} ->
              nil

            {:error, _reason} = error ->
              error

            _meta ->
              with :ok <- TypeRegistry.check_or_set(destination, :list, store) do
                ListOps.execute_lmove(source, destination, store, from_dir, to_dir)
              end
          end
        end
      end
    end
  end
end
