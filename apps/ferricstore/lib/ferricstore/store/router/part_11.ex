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
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.SlotMap
      alias Ferricstore.Store.TypeRegistry

      @spec flow_index_count_all_many(FerricStore.Instance.t(), [binary()]) ::
              {:ok, [non_neg_integer()]} | :unavailable
      def flow_index_count_all_many(_ctx, []), do: {:ok, []}

      def flow_index_count_all_many(ctx, [first_key | _] = keys) do
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
      end

      defp flow_index_count_all_many_cross_shard(ctx, keys) do
        Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
          case flow_index_count_all(ctx, key) do
            {:ok, count} -> {:cont, {:ok, [count | acc]}}
            :unavailable -> {:halt, :unavailable}
          end
        end)
        |> case do
          {:ok, counts} -> {:ok, Enum.reverse(counts)}
          :unavailable -> :unavailable
        end
      end

      @doc false
      @spec flow_due_count_keys(FerricStore.Instance.t()) :: {:ok, [binary()]} | :unavailable
      def flow_due_count_keys(ctx) do
        shard_count = max(1, Map.get(ctx, :shard_count, 1))

        0..(shard_count - 1)//1
        |> Enum.reduce_while({:ok, []}, fn idx, {:ok, acc} ->
          case flow_due_count_keys(ctx, idx) do
            {:ok, keys} when is_list(keys) -> {:cont, {:ok, [keys | acc]}}
            :unavailable -> {:halt, :unavailable}
            _other -> {:halt, :unavailable}
          end
        end)
        |> case do
          {:ok, key_groups} ->
            {:ok, key_groups |> Enum.reverse() |> List.flatten() |> Enum.uniq()}

          :unavailable ->
            :unavailable
        end
      end

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
          {:ok, direct_zset_rank_range(ctx, idx, redis_key, start_idx, stop_idx, reverse?)}
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
          {:ok, direct_zset_member_rank(ctx, idx, redis_key, member, reverse?)}
        else
          ctx
          |> safe_read_call(idx, {:zset_member_rank, redis_key, member, reverse?})
          |> unwrap_zset_index_reply()
        end
      end

      defp unwrap_zset_index_reply({:ok, {:ok, result}}), do: {:ok, result}
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
        Enum.reduce_while(requests, {:ok, []}, fn {key, start_idx, stop_idx, reverse?},
                                                  {:ok, acc} ->
          idx = shard_for(ctx, key)

          case direct_flow_index_rank_range(ctx, idx, key, start_idx, stop_idx, reverse?) do
            {:ok, result} -> {:cont, {:ok, [result | acc]}}
            :unavailable -> {:halt, :unavailable}
          end
        end)
        |> case do
          {:ok, results} -> {:ok, Enum.reverse(results)}
          :unavailable -> :unavailable
        end
      end

      defp direct_flow_index_count_all(ctx, idx, key) do
        direct_flow_index_read(ctx, idx, fn native ->
          NativeFlowIndex.count_all(native, key)
        end)
      end

      defp direct_flow_index_count_all_many(ctx, keys) do
        Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
          idx = shard_for(ctx, key)

          case direct_flow_index_count_all(ctx, idx, key) do
            {:ok, count} -> {:cont, {:ok, [count | acc]}}
            :unavailable -> {:halt, :unavailable}
          end
        end)
        |> case do
          {:ok, counts} -> {:ok, Enum.reverse(counts)}
          :unavailable -> :unavailable
        end
      end

      defp flow_due_count_keys(ctx, idx) do
        if selected_waraft_ctx?(ctx) do
          direct_flow_index_read(ctx, idx, &NativeFlowIndex.due_count_keys/1)
        else
          ctx
          |> safe_read_call(idx, :flow_due_count_keys)
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

      defp direct_zset_score_range(ctx, idx, redis_key, min_bound, max_bound, reverse?) do
        ctx
        |> direct_zset_sorted_members(idx, redis_key, reverse?)
        |> Enum.filter(fn {_member, score} ->
          zset_score_gte_bound?(score, min_bound) and zset_score_lte_bound?(score, max_bound)
        end)
      end

      defp direct_zset_score_count(ctx, idx, redis_key, min_bound, max_bound) do
        ctx
        |> direct_zset_score_range(idx, redis_key, min_bound, max_bound, false)
        |> length()
      end

      defp direct_zset_rank_range(_ctx, _idx, _redis_key, start_idx, stop_idx, _reverse?)
           when start_idx > stop_idx,
           do: []

      defp direct_zset_rank_range(ctx, idx, redis_key, start_idx, stop_idx, reverse?) do
        ctx
        |> direct_zset_sorted_members(idx, redis_key, reverse?)
        |> Enum.slice(start_idx..stop_idx)
      end

      defp direct_zset_member_rank(ctx, idx, redis_key, member, reverse?) do
        ctx
        |> direct_zset_sorted_members(idx, redis_key, reverse?)
        |> Enum.find_index(fn {candidate, _score} -> candidate == member end)
      end

      defp direct_zset_sorted_members(ctx, idx, redis_key, false) do
        ctx
        |> direct_zset_members(idx, redis_key)
        |> Enum.sort_by(fn {member, score} -> {score, member} end)
      end

      defp direct_zset_sorted_members(ctx, idx, redis_key, true) do
        ctx
        |> direct_zset_sorted_members(idx, redis_key, false)
        |> Enum.reverse()
      end

      defp direct_zset_members(ctx, idx, redis_key) do
        idx
        |> direct_compound_scan(ctx, CompoundKey.zset_prefix(redis_key))
        |> Enum.flat_map(fn {member, score_str} ->
          case Float.parse(score_str) do
            {score, ""} -> [{member, score}]
            _ -> []
          end
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
            intent: %{command: :lmove, keys: %{source: key, dest: destination}},
            tx_entry:
              {"LMOVE", [key, destination, Atom.to_string(from_dir), Atom.to_string(to_dir)],
               {:lmove, key, destination, from_dir, to_dir}},
            instance: ctx
          )
        end
      end

      def list_op(ctx, key, operation) do
        idx = shard_for(ctx, key)
        raft_write(ctx, idx, key, {:list_op, key, operation})
      end

      defp checked_lmove(source, destination, store, from_dir, to_dir) do
        with :ok <- TypeRegistry.check_type(source, :list, store) do
          case ListOps.read_meta(source, store) do
            nil ->
              nil

            {0, _, _} ->
              nil

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
