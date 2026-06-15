defmodule Ferricstore.Store.Router.Part07 do
  @moduledoc false

  # Extracted from Router: flow_claim_due .. flow_transition_batch_valid_results
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
      @doc false
      def flow_claim_due(ctx, %{partition_key: :auto, limit: limit} = attrs)
          when is_integer(limit) and limit > 0 do
        start_idx = flow_claim_due_start_shard(ctx, attrs)

        flow_claim_due_auto_priorities(
          ctx,
          attrs,
          start_idx,
          flow_claim_any_priorities(Map.get(attrs, :priority)),
          limit,
          []
        )
      end

      def flow_claim_due(ctx, %{partition_keys: [_ | _] = partition_keys, limit: limit} = attrs)
          when is_integer(limit) and limit > 0 do
        flow_claim_due_partition_keys(ctx, attrs, Enum.uniq(partition_keys), limit)
      end

      def flow_claim_due(ctx, %{partition_key: partition_key, limit: limit} = attrs)
          when partition_key == :any and is_integer(limit) and limit > 0 do
        start_idx = flow_claim_due_start_shard(ctx, attrs)

        flow_claim_due_any_priorities(
          ctx,
          attrs,
          start_idx,
          flow_claim_any_priorities(Map.get(attrs, :priority)),
          limit,
          []
        )
      end

      def flow_claim_due(ctx, %{type: type, priority: priority} = attrs)
          when is_binary(type) and (is_integer(priority) or is_nil(priority)) do
        requested_state = Map.get(attrs, :state)
        state = flow_claim_route_state(requested_state)
        partition_key = Map.get(attrs, :partition_key)

        key =
          Ferricstore.Flow.Keys.due_key(type, state, priority || 0, partition_key)

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)

          case flow_claim_due_empty_precheck(
                 ctx,
                 idx,
                 type,
                 requested_state,
                 priority,
                 partition_key,
                 attrs
               ) do
            :empty ->
              {:ok, []}

            {:non_empty, cold_due_mode} ->
              raft_write(
                ctx,
                idx,
                key,
                {:flow_claim_due, key, Map.put(attrs, :cold_due_mode, cold_due_mode)}
              )

            _unknown ->
              raft_write(ctx, idx, key, {:flow_claim_due, key, attrs})
          end
        end
      end

      defp flow_claim_due_auto_partition(ctx, attrs, start_idx, limit) do
        flow_claim_due_auto_partition(ctx, attrs, start_idx, limit, 0, [])
      end

      defp flow_claim_due_auto_partition(_ctx, _attrs, _start_idx, remaining, _offset, acc)
           when remaining <= 0,
           do: {:ok, Enum.reverse(acc)}

      defp flow_claim_due_auto_partition(ctx, _attrs, _start_idx, _remaining, offset, acc)
           when offset >= ctx.shard_count,
           do: {:ok, Enum.reverse(acc)}

      defp flow_claim_due_auto_partition(ctx, attrs, start_idx, remaining, offset, acc) do
        idx = rem(start_idx + offset, ctx.shard_count)

        case flow_claim_due_auto_empty_precheck(ctx, idx, attrs) do
          :empty ->
            flow_claim_due_auto_partition(ctx, attrs, start_idx, remaining, offset + 1, acc)

          {:non_empty, cold_due_mode} ->
            attrs
            |> Map.put(:cold_due_mode, cold_due_mode)
            |> flow_claim_due_auto_partition_write(ctx, idx, start_idx, remaining, offset, acc)

          _unknown ->
            flow_claim_due_auto_partition_write(
              attrs,
              ctx,
              idx,
              start_idx,
              remaining,
              offset,
              acc
            )
        end
      end

      defp flow_claim_due_auto_partition_write(attrs, ctx, idx, start_idx, remaining, offset, acc) do
        key = "f:{flow-claim-auto-" <> Integer.to_string(idx) <> "}:d"
        shard_attrs = Map.put(attrs, :limit, remaining)

        case raft_write(ctx, idx, key, {:flow_claim_due, key, shard_attrs}) do
          {:ok, []} ->
            flow_claim_due_auto_partition(ctx, attrs, start_idx, remaining, offset + 1, acc)

          {:ok, records} when is_list(records) ->
            flow_claim_due_auto_partition(
              ctx,
              attrs,
              start_idx,
              remaining - length(records),
              offset + 1,
              Enum.reverse(records, acc)
            )

          {:error, _reason} = error ->
            error

          other ->
            other
        end
      end

      defp flow_claim_due_partition_keys(ctx, attrs, partition_keys, limit) do
        type = Map.fetch!(attrs, :type)
        state = flow_claim_route_state(Map.get(attrs, :state))
        priority = Map.get(attrs, :priority) || 0
        start_idx = flow_claim_due_start_shard(ctx, attrs)

        groups =
          partition_keys
          |> Enum.group_by(fn partition_key ->
            key = Ferricstore.Flow.Keys.due_key(type, state, priority, partition_key)
            shard_for(ctx, key)
          end)
          |> Enum.sort_by(fn {idx, _keys} ->
            rem(idx - start_idx + ctx.shard_count, ctx.shard_count)
          end)

        if limit == 1 do
          flow_claim_due_partition_key_groups_one(ctx, groups, attrs)
        else
          case flow_claim_due_partition_key_commands(ctx, groups, attrs, limit) do
            [] ->
              {:ok, []}

            [{key, command}] ->
              case raft_write(ctx, shard_for(ctx, key), key, command) do
                {:ok, records} when is_list(records) -> {:ok, Enum.take(records, limit)}
                other -> other
              end

            commands ->
              ctx
              |> pipeline_write_batch(commands)
              |> flow_claim_due_partition_key_results(limit)
          end
        end
      end

      defp flow_claim_due_partition_key_groups_one(_ctx, [], _attrs), do: {:ok, []}

      defp flow_claim_due_partition_key_groups_one(ctx, [group | rest], attrs) do
        case flow_claim_due_partition_key_commands(ctx, [group], attrs, 1) do
          [] ->
            flow_claim_due_partition_key_groups_one(ctx, rest, attrs)

          [{key, command}] ->
            case raft_write(ctx, shard_for(ctx, key), key, command) do
              {:ok, []} -> flow_claim_due_partition_key_groups_one(ctx, rest, attrs)
              {:ok, records} when is_list(records) -> {:ok, Enum.take(records, 1)}
              other -> other
            end

          commands ->
            case pipeline_write_batch(ctx, commands) |> flow_claim_due_partition_key_results(1) do
              {:ok, []} -> flow_claim_due_partition_key_groups_one(ctx, rest, attrs)
              other -> other
            end
        end
      end

      defp flow_claim_due_partition_key_commands(_ctx, [], _attrs, _limit), do: []

      defp flow_claim_due_partition_key_commands(ctx, groups, attrs, limit) do
        type = Map.fetch!(attrs, :type)
        requested_state = Map.get(attrs, :state)
        state = flow_claim_route_state(requested_state)
        priority = Map.get(attrs, :priority)
        route_priority = priority || 0

        groups =
          groups
          |> Enum.flat_map(fn {idx, partition_keys} ->
            {filtered, modes} =
              Enum.reduce(partition_keys, {[], []}, fn partition_key, {keys_acc, modes_acc} ->
                case flow_claim_due_empty_precheck(
                       ctx,
                       idx,
                       type,
                       requested_state,
                       priority,
                       partition_key,
                       attrs
                     ) do
                  :empty ->
                    {keys_acc, modes_acc}

                  {:non_empty, mode} ->
                    {[partition_key | keys_acc], [mode | modes_acc]}

                  _unknown ->
                    {[partition_key | keys_acc],
                     [Map.get(attrs, :cold_due_mode, :skip) | modes_acc]}
                end
              end)

            if filtered == [] do
              []
            else
              [{idx, Enum.reverse(filtered), flow_claim_due_group_cold_mode(modes, attrs)}]
            end
          end)

        group_count = length(groups)

        if group_count == 0 do
          []
        else
          base = div(limit, group_count)
          extra = rem(limit, group_count)

          groups
          |> Enum.with_index()
          |> Enum.flat_map(fn {{_idx, partition_keys, cold_due_mode}, group_idx} ->
            quota = base + if(group_idx < extra, do: 1, else: 0)

            if quota <= 0 do
              []
            else
              key = Ferricstore.Flow.Keys.due_key(type, state, route_priority, hd(partition_keys))

              shard_attrs =
                attrs
                |> Map.put(:limit, quota)
                |> Map.put(:partition_key, hd(partition_keys))
                |> Map.put(:partition_keys, partition_keys)
                |> Map.put(:cold_due_mode, cold_due_mode)

              [{key, {:flow_claim_due, key, shard_attrs}}]
            end
          end)
        end
      end

      defp flow_claim_due_group_cold_mode(modes, attrs) do
        cond do
          Enum.any?(modes, &(&1 == :skip)) -> :skip
          Enum.any?(modes, &(&1 == :allow)) -> :allow
          true -> Map.get(attrs, :cold_due_mode, :skip)
        end
      end

      defp flow_claim_due_empty_precheck(ctx, idx, type, state, priority, partition_key, attrs) do
        with true <- flow_claim_due_empty_precheck_allowed?(ctx, idx),
             {:ok, due_keys} <- flow_claim_due_precheck_keys(type, state, priority, partition_key),
             true <- due_keys != [],
             {:ok, native} <- direct_flow_index_read(ctx, idx, & &1) do
          now_ms = flow_claim_due_precheck_now_ms(attrs)

          case NativeFlowIndex.due_keys_present(native, due_keys, now_ms) do
            [] ->
              if flow_claim_due_cold_precheck_present?(
                   ctx,
                   idx,
                   type,
                   state,
                   priority,
                   partition_key,
                   now_ms,
                   Map.get(attrs, :cold_due_mode)
                 ) do
                {:non_empty, :allow}
              else
                :empty
              end

            [_ | _] ->
              {:non_empty, :skip}
          end
        else
          _other -> :unknown
        end
      rescue
        _error -> :unknown
      catch
        _kind, _reason -> :unknown
      end

      defp flow_claim_due_auto_empty_precheck(ctx, idx, attrs) do
        type = Map.fetch!(attrs, :type)
        state = Map.get(attrs, :state)
        priority = Map.get(attrs, :priority)

        with true <- flow_claim_due_empty_precheck_allowed?(ctx, idx),
             {:ok, due_keys} <- flow_claim_due_auto_precheck_keys(ctx, idx, type, state, priority),
             true <- due_keys != [],
             {:ok, native} <- direct_flow_index_read(ctx, idx, & &1) do
          now_ms = flow_claim_due_precheck_now_ms(attrs)

          case NativeFlowIndex.due_keys_present(native, due_keys, now_ms) do
            [] ->
              if flow_claim_due_cold_precheck_present?(
                   ctx,
                   idx,
                   type,
                   state,
                   priority,
                   :auto,
                   now_ms,
                   Map.get(attrs, :cold_due_mode)
                 ) do
                {:non_empty, :allow}
              else
                :empty
              end

            [_ | _] ->
              {:non_empty, :skip}
          end
        else
          _other -> :unknown
        end
      rescue
        _error -> :unknown
      catch
        _kind, _reason -> :unknown
      end

      defp flow_claim_due_precheck_now_ms(%{now_ms: now_ms}) when is_integer(now_ms), do: now_ms

      defp flow_claim_due_precheck_now_ms(_attrs),
        do: CommandTime.now_ms() + @flow_claim_due_precheck_slack_ms

      defp flow_claim_due_cold_precheck_present?(
             _ctx,
             _idx,
             _type,
             "running",
             _priority,
             _partition_key,
             _now_ms,
             _cold_due_mode
           ),
           do: false

      defp flow_claim_due_cold_precheck_present?(
             ctx,
             idx,
             type,
             state,
             priority,
             partition_key,
             now_ms,
             cold_due_mode
           ) do
        with true <- cold_due_mode in [:allow, :block],
             true <- Ferricstore.Flow.Hibernation.enabled?(),
             {:ok, states} <- flow_claim_due_precheck_states(state),
             priorities <- flow_claim_any_priorities(priority),
             true <- Enum.all?(priorities, &is_integer/1),
             path when is_binary(path) <- flow_claim_due_cold_precheck_path(ctx, idx),
             [_ | _] = buckets <- flow_claim_due_cold_precheck_buckets(now_ms) do
          Enum.any?(states, fn claim_state ->
            Enum.any?(priorities, fn claim_priority ->
              Enum.any?(buckets, fn bucket_ms ->
                flow_claim_due_cold_precheck_partition_present?(
                  ctx,
                  idx,
                  path,
                  bucket_ms,
                  type,
                  claim_state,
                  claim_priority,
                  partition_key
                )
              end)
            end)
          end)
        else
          _ -> false
        end
      rescue
        _ -> false
      catch
        _kind, _reason -> false
      end

      defp flow_claim_due_cold_precheck_path(%{data_dir: data_dir}, idx)
           when is_binary(data_dir) and is_integer(idx) and idx >= 0 do
        data_dir
        |> Ferricstore.DataDir.shard_data_path(idx)
        |> Ferricstore.Flow.LMDB.path()
      end

      defp flow_claim_due_cold_precheck_path(_ctx, _idx), do: nil

      defp flow_claim_due_cold_precheck_buckets(now_ms) when is_integer(now_ms) and now_ms >= 0 do
        first =
          now_ms
          |> Kernel.-(Ferricstore.Flow.Hibernation.late_promote_window_ms())
          |> max(0)
          |> Ferricstore.Flow.LMDB.cold_due_bucket_ms()

        last =
          now_ms
          |> Kernel.+(Ferricstore.Flow.Hibernation.promote_window_ms())
          |> Ferricstore.Flow.LMDB.cold_due_bucket_ms()

        first
        |> Stream.iterate(&(&1 + 60_000))
        |> Stream.take_while(&(&1 <= last))
        |> Enum.to_list()
      end

      defp flow_claim_due_cold_precheck_buckets(_now_ms), do: []

      defp flow_claim_due_cold_precheck_partition_present?(
             _ctx,
             _idx,
             path,
             bucket_ms,
             type,
             claim_state,
             _claim_priority,
             :auto
           ) do
        prefix = Ferricstore.Flow.LMDB.cold_due_state_bucket_prefix(bucket_ms, type, claim_state)

        case Ferricstore.Flow.LMDB.prefix_entries(path, prefix, 1) do
          {:ok, [_ | _]} -> true
          _ -> false
        end
      end

      defp flow_claim_due_cold_precheck_partition_present?(
             _ctx,
             _idx,
             path,
             bucket_ms,
             type,
             claim_state,
             claim_priority,
             partition_key
           ) do
        prefix =
          Ferricstore.Flow.LMDB.cold_due_claim_prefix(
            bucket_ms: bucket_ms,
            type: type,
            state: claim_state,
            partition_key: partition_key,
            priority: claim_priority
          )

        case Ferricstore.Flow.LMDB.prefix_entries(path, prefix, 1) do
          {:ok, [_ | _]} -> true
          _ -> false
        end
      end

      defp flow_claim_due_empty_precheck_allowed?(ctx, idx) do
        selected_waraft_ctx?(ctx) and flow_claim_due_single_local_member?(idx)
      end

      defp flow_claim_due_single_local_member?(idx) do
        case Ferricstore.Raft.WARaftBackend.cached_members(idx) do
          {:ok, [{_server, node_name}], nil} ->
            node_name == node()

          {:ok, [{_server, node_name}], {_leader_server, leader_node}} ->
            node_name == node() and leader_node == node()

          _other ->
            false
        end
      catch
        _kind, _reason -> false
      end

      defp flow_claim_due_precheck_keys(type, state, priority, partition_key)
           when is_binary(type) and partition_key not in [:any, :auto] do
        with {:ok, states} <- flow_claim_due_precheck_states(state),
             priorities <- flow_claim_any_priorities(priority),
             true <- Enum.all?(priorities, &is_integer/1) do
          keys =
            for state <- states,
                priority <- priorities do
              Ferricstore.Flow.Keys.due_key(type, state, priority, partition_key)
            end

          {:ok, keys}
        else
          _other -> :unknown
        end
      end

      defp flow_claim_due_precheck_keys(_type, _state, _priority, _partition_key), do: :unknown

      defp flow_claim_due_precheck_states(state) when is_binary(state), do: {:ok, [state]}

      defp flow_claim_due_precheck_states(states) when is_list(states) do
        if Enum.all?(states, &is_binary/1), do: {:ok, states}, else: :unknown
      end

      defp flow_claim_due_precheck_states(_state), do: :unknown

      defp flow_claim_due_auto_precheck_keys(ctx, idx, type, state, priority)
           when is_binary(type) do
        with {:ok, states} <- flow_claim_due_precheck_states(state),
             priorities <- flow_claim_any_priorities(priority),
             true <- Enum.all?(priorities, &is_integer/1),
             [_ | _] = partition_keys <- flow_auto_partition_keys_for_shard(ctx, idx) do
          keys =
            for state <- states,
                priority <- priorities,
                partition_key <- partition_keys do
              Ferricstore.Flow.Keys.due_key(type, state, priority, partition_key)
            end

          {:ok, keys}
        else
          _other -> :unknown
        end
      end

      defp flow_claim_due_auto_precheck_keys(_ctx, _idx, _type, _state, _priority), do: :unknown

      defp flow_auto_partition_keys_for_shard(%{shard_count: shard_count} = ctx, idx)
           when is_integer(shard_count) and shard_count > 0 and is_integer(idx) do
        key = {__MODULE__, :flow_auto_partition_keys_for_shard, ctx.name, ctx.slot_map}

        key
        |> :persistent_term.get(nil)
        |> case do
          nil ->
            groups =
              Ferricstore.Flow.Keys.auto_partition_keys()
              |> Enum.group_by(fn partition_key ->
                "__auto_probe__"
                |> Ferricstore.Flow.Keys.due_key("__auto_probe__", 0, partition_key)
                |> then(&shard_for(ctx, &1))
              end)

            :persistent_term.put(key, groups)
            Map.get(groups, idx, [])

          groups ->
            Map.get(groups, idx, [])
        end
      end

      defp flow_auto_partition_keys_for_shard(_ctx, _idx), do: []

      defp flow_claim_due_partition_key_results(results, limit) do
        results
        |> Enum.reduce_while({:ok, []}, fn
          {:ok, records}, {:ok, acc} when is_list(records) ->
            {:cont, {:ok, Enum.reverse(records, acc)}}

          {:error, _reason} = error, {:ok, _acc} ->
            {:halt, error}

          other, {:ok, _acc} ->
            {:halt, other}
        end)
        |> case do
          {:ok, records} -> {:ok, records |> Enum.reverse() |> Enum.take(limit)}
          other -> other
        end
      end

      defp flow_claim_due_auto_priorities(ctx, attrs, start_idx, priorities, limit, acc) do
        flow_claim_due_auto_priorities(ctx, attrs, start_idx, priorities, limit, acc, length(acc))
      end

      defp flow_claim_due_auto_priorities(_ctx, _attrs, _start_idx, [], _limit, acc, _count),
        do: {:ok, Enum.reverse(acc)}

      defp flow_claim_due_auto_priorities(
             _ctx,
             _attrs,
             _start_idx,
             _priorities,
             limit,
             acc,
             count
           )
           when count >= limit,
           do: {:ok, acc |> Enum.reverse() |> Enum.take(limit)}

      defp flow_claim_due_auto_priorities(
             ctx,
             attrs,
             start_idx,
             [priority | rest],
             limit,
             acc,
             count
           ) do
        remaining = limit - count
        priority_attrs = %{attrs | priority: priority, limit: remaining}

        case flow_claim_due_auto_partition(ctx, priority_attrs, start_idx, remaining) do
          {:ok, records} ->
            record_count = length(records)

            flow_claim_due_auto_priorities(
              ctx,
              attrs,
              start_idx,
              rest,
              limit,
              Enum.reduce(records, acc, fn record, next -> [record | next] end),
              count + record_count
            )

          {:error, _reason} = error when count == 0 ->
            error

          {:error, _reason} ->
            {:ok, Enum.reverse(acc)}

          other ->
            other
        end
      end

      defp flow_claim_due_any_partition(ctx, attrs, start_idx, _offset, limit, _acc) do
        flow_claim_due_any_partition_rounds(ctx, attrs, start_idx, limit, [], 0)
      end

      defp flow_claim_due_any_partition_rounds(_ctx, _attrs, _start_idx, limit, acc, count)
           when count >= limit,
           do: {:ok, acc |> Enum.reverse() |> Enum.take(limit)}

      defp flow_claim_due_any_partition_rounds(ctx, attrs, start_idx, limit, acc, count) do
        remaining = limit - count

        per_shard_limit =
          max(
            1,
            div(
              remaining * @flow_claim_due_any_window_multiplier + ctx.shard_count - 1,
              ctx.shard_count
            )
          )

        case flow_claim_due_any_partition_round(
               ctx,
               attrs,
               start_idx,
               0,
               remaining,
               per_shard_limit,
               [],
               0,
               false
             ) do
          {:ok, [], 0, _progressed?} ->
            {:ok, Enum.reverse(acc)}

          {:ok, records, record_count, true} ->
            flow_claim_due_any_partition_rounds(
              ctx,
              attrs,
              start_idx,
              limit,
              Enum.reduce(records, acc, fn record, next -> [record | next] end),
              count + record_count
            )

          {:error, _reason} = error when count == 0 ->
            error

          {:error, _reason} ->
            {:ok, Enum.reverse(acc)}

          other ->
            other
        end
      end

      defp flow_claim_due_any_partition_round(
             ctx,
             _attrs,
             _start_idx,
             offset,
             _remaining,
             _per_shard_limit,
             acc,
             acc_count,
             progressed?
           )
           when offset >= ctx.shard_count,
           do: {:ok, Enum.reverse(acc), acc_count, progressed?}

      defp flow_claim_due_any_partition_round(
             _ctx,
             _attrs,
             _start_idx,
             _offset,
             remaining,
             _per_shard_limit,
             acc,
             acc_count,
             progressed?
           )
           when acc_count >= remaining,
           do:
             {:ok, acc |> Enum.reverse() |> Enum.take(remaining), min(acc_count, remaining),
              progressed?}

      defp flow_claim_due_any_partition_round(
             ctx,
             attrs,
             start_idx,
             offset,
             remaining,
             per_shard_limit,
             acc,
             acc_count,
             progressed?
           ) do
        idx = rem(start_idx + offset, ctx.shard_count)
        shard_remaining = remaining - acc_count
        shard_limit = min(per_shard_limit, shard_remaining)
        key = "f:{flow-claim-any-" <> Integer.to_string(idx) <> "}:d"
        shard_attrs = Map.put(attrs, :limit, shard_limit)

        case raft_write(ctx, idx, key, {:flow_claim_due, key, shard_attrs}) do
          {:ok, records} when is_list(records) ->
            record_count = length(records)

            flow_claim_due_any_partition_round(
              ctx,
              attrs,
              start_idx,
              offset + 1,
              remaining,
              per_shard_limit,
              Enum.reduce(records, acc, fn record, next -> [record | next] end),
              acc_count + record_count,
              progressed? or records != []
            )

          {:error, _reason} = error when acc_count == 0 ->
            error

          {:error, _reason} ->
            {:ok, Enum.reverse(acc), acc_count, progressed?}

          other ->
            other
        end
      end

      defp flow_claim_due_any_priorities(ctx, attrs, start_idx, priorities, limit, acc) do
        flow_claim_due_any_priorities(ctx, attrs, start_idx, priorities, limit, acc, length(acc))
      end

      defp flow_claim_due_any_priorities(_ctx, _attrs, _start_idx, [], _limit, acc, _count),
        do: {:ok, Enum.reverse(acc)}

      defp flow_claim_due_any_priorities(_ctx, _attrs, _start_idx, _priorities, limit, acc, count)
           when count >= limit,
           do: {:ok, acc |> Enum.reverse() |> Enum.take(limit)}

      defp flow_claim_due_any_priorities(
             ctx,
             attrs,
             start_idx,
             [priority | rest],
             limit,
             acc,
             count
           ) do
        remaining = limit - count
        priority_attrs = %{attrs | priority: priority, limit: remaining}

        case flow_claim_due_any_partition(ctx, priority_attrs, start_idx, 0, remaining, []) do
          {:ok, records} ->
            record_count = length(records)

            flow_claim_due_any_priorities(
              ctx,
              attrs,
              start_idx,
              rest,
              limit,
              Enum.reduce(records, acc, fn record, next -> [record | next] end),
              count + record_count
            )

          {:error, _reason} = error when count == 0 ->
            error

          {:error, _reason} ->
            {:ok, Enum.reverse(acc)}

          other ->
            other
        end
      end

      defp flow_claim_any_priorities(nil), do: [2, 1, 0]
      defp flow_claim_any_priorities(priority), do: [priority]

      defp flow_claim_due_start_shard(%{shard_count: shard_count} = ctx, attrs)
           when shard_count > 1 do
        table = flow_claim_due_cursor_table()
        type = Map.get(attrs, :type, "")
        cursor_key = {ctx.name, type}

        next =
          :ets.update_counter(table, cursor_key, {2, 1}, {cursor_key, 0})

        rem(next - 1, shard_count)
      end

      defp flow_claim_due_start_shard(_ctx, _attrs), do: 0

      defp flow_claim_due_cursor_table do
        case :ets.whereis(@flow_claim_cursor_table) do
          :undefined ->
            try do
              :ets.new(@flow_claim_cursor_table, [
                :named_table,
                :public,
                read_concurrency: true,
                write_concurrency: true
              ])
            rescue
              ArgumentError -> @flow_claim_cursor_table
            end

          _tid ->
            @flow_claim_cursor_table
        end
      end

      defp flow_claim_route_state(:any), do: "queued"
      defp flow_claim_route_state([state | _]) when is_binary(state), do: state
      defp flow_claim_route_state(state) when is_binary(state), do: state
      defp flow_claim_route_state(_state), do: "queued"

      @doc false
      def flow_extend_lease(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:flow_extend_lease, key, attrs})
        end
      end

      @doc false
      def flow_complete(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          case flow_cross_terminal_keys(ctx, id, Map.get(attrs, :partition_key)) do
            {:ok, keys} ->
              flow_cross_shard_tx(ctx, keys, {:flow_cross_terminal, :complete, attrs})

            :same_or_none ->
              idx = shard_for(ctx, key)
              raft_write(ctx, idx, key, {:flow_complete, key, attrs})
          end
        end
      end

      @doc false
      def flow_complete_many(ctx, partition_key, attrs_list)
          when is_binary(partition_key) and is_list(attrs_list) do
        key = Ferricstore.Flow.Keys.state_key("__complete_batch__", partition_key)

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          case flow_cross_terminal_many_keys(ctx, attrs_list) do
            {:ok, keys} ->
              flow_cross_shard_tx(
                ctx,
                keys,
                {:flow_cross_terminal_many, :complete, %{records: attrs_list}}
              )

            :same_or_none ->
              idx = shard_for(ctx, key)

              raft_write(
                ctx,
                idx,
                key,
                {:flow_complete_many, key, %{records: attrs_list}}
              )
          end
        end
      end

      def flow_complete_many(ctx, nil, attrs_list) when is_list(attrs_list) do
        case flow_cross_terminal_many_keys(ctx, attrs_list) do
          {:ok, keys} ->
            flow_cross_shard_tx(
              ctx,
              keys,
              {:flow_cross_terminal_many, :complete, %{records: attrs_list}}
            )

          :same_or_none ->
            flow_many_by_shard(ctx, attrs_list, :flow_complete_many, "__complete_batch__")
        end
      end

      @doc false
      def flow_complete_many_local(ctx, attrs_list) when is_list(attrs_list) do
        flow_many_by_shard(ctx, attrs_list, :flow_complete_many, "__complete_batch__")
      end

      @doc false
      def flow_transition(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:flow_transition, key, attrs})
        end
      end

      @doc false
      def flow_reschedule(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:flow_reschedule, key, attrs})
        end
      end

      @doc false
      def flow_schedule_replace(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:flow_schedule_replace, key, attrs})
        end
      end

      @doc false
      def flow_start_and_claim(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:flow_start_and_claim, key, attrs})
        end
      end

      @doc false
      def flow_run_steps_many(_ctx, []), do: :ok

      def flow_run_steps_many(ctx, attrs_list) when is_list(attrs_list) do
        flow_many_by_shard(ctx, attrs_list, :flow_run_steps_many, "__run_steps_batch__")
      end

      @doc false
      def flow_step_continue(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:flow_step_continue, key, attrs})
        end
      end

      @doc false
      def flow_transition_batch(_ctx, []), do: []

      def flow_transition_batch(ctx, attrs_list) when is_list(attrs_list) do
        if ctx.name == :default do
          {valid, indexed_results} =
            attrs_list
            |> Enum.with_index()
            |> Enum.reduce({[], %{}}, fn
              {%{id: id} = attrs, idx}, {valid_acc, result_acc} when is_binary(id) ->
                key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

                if byte_size(key) > @max_key_size do
                  {valid_acc,
                   Map.put(
                     result_acc,
                     idx,
                     {:error, "ERR key too large (max #{@max_key_size} bytes)"}
                   )}
                else
                  {[{idx, key, {:flow_transition, key, attrs}} | valid_acc], result_acc}
                end

              {_attrs, idx}, {valid_acc, result_acc} ->
                {valid_acc,
                 Map.put(result_acc, idx, {:error, "ERR flow id must be a non-empty string"})}
            end)

          valid = Enum.reverse(valid)

          valid_results = flow_transition_batch_valid_results(ctx, valid)

          indexed_results =
            valid
            |> Enum.map(fn {idx, _key, _cmd} -> idx end)
            |> Enum.zip(valid_results)
            |> Enum.reduce(indexed_results, fn {idx, result}, acc -> Map.put(acc, idx, result) end)

          for idx <- 0..(length(attrs_list) - 1), do: Map.fetch!(indexed_results, idx)
        else
          Enum.map(attrs_list, &flow_transition(ctx, &1))
        end
      end

      defp flow_transition_batch_valid_results(_ctx, []), do: []

      defp flow_transition_batch_valid_results(ctx, valid) do
        {buckets, count} =
          valid
          |> Enum.with_index()
          |> Enum.reduce({flow_fixed_shard_buckets(ctx.shard_count), 0}, fn
            {{_idx, key, {:flow_transition, _key, attrs}}, local_idx}, {buckets, count} ->
              shard_idx = shard_for(ctx, key)
              {flow_put_shard_bucket(buckets, shard_idx, {local_idx, key, attrs}), count + 1}
          end)

        groups =
          flow_nonempty_shard_buckets(buckets, ctx.shard_count, fn shard_idx, entries ->
            group = Enum.reverse(entries)
            key = group |> hd() |> elem(1)
            local_indices = Enum.map(group, fn {idx, _key, _attrs} -> idx end)
            attrs_list = Enum.map(group, fn {_idx, _key, attrs} -> attrs end)

            command_attrs =
              attrs_list
              |> flow_transition_many_command_attrs()
              |> Map.put(:independent, true)
              |> flow_stamp_shard(shard_idx)

            {key, local_indices, {:flow_transition_many, key, command_attrs}}
          end)

        keyed_commands = Enum.map(groups, fn {key, _indices, cmd} -> {key, cmd} end)
        group_results = batch_quorum_commands(ctx, keyed_commands)
        expand_flow_transition_batch_results(count, groups, group_results)
      end

      def flow_step_continue_batch(_ctx, []), do: []

      def flow_step_continue_batch(ctx, attrs_list) when is_list(attrs_list) do
        if ctx.name == :default do
          {valid, indexed_results} =
            attrs_list
            |> Enum.with_index()
            |> Enum.reduce({[], %{}}, fn
              {%{id: id} = attrs, idx}, {valid_acc, result_acc} when is_binary(id) ->
                key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

                if byte_size(key) > @max_key_size do
                  {valid_acc,
                   Map.put(
                     result_acc,
                     idx,
                     {:error, "ERR key too large (max #{@max_key_size} bytes)"}
                   )}
                else
                  {[{idx, key, {:flow_step_continue, key, attrs}} | valid_acc], result_acc}
                end

              {_attrs, idx}, {valid_acc, result_acc} ->
                {valid_acc,
                 Map.put(result_acc, idx, {:error, "ERR flow id must be a non-empty string"})}
            end)

          valid = Enum.reverse(valid)
          valid_results = flow_step_continue_batch_valid_results(ctx, valid)

          indexed_results =
            valid
            |> Enum.map(fn {idx, _key, _cmd} -> idx end)
            |> Enum.zip(valid_results)
            |> Enum.reduce(indexed_results, fn {idx, result}, acc -> Map.put(acc, idx, result) end)

          for idx <- 0..(length(attrs_list) - 1), do: Map.fetch!(indexed_results, idx)
        else
          attrs_list
          |> Enum.map(fn attrs ->
            key =
              Ferricstore.Flow.Keys.state_key(Map.get(attrs, :id), Map.get(attrs, :partition_key))

            raft_write(ctx, shard_for(ctx, key), key, {:flow_step_continue, key, attrs})
          end)
        end
      end

      defp flow_step_continue_batch_valid_results(_ctx, []), do: []

      defp flow_step_continue_batch_valid_results(ctx, valid) do
        {buckets, count} =
          valid
          |> Enum.with_index()
          |> Enum.reduce({flow_fixed_shard_buckets(ctx.shard_count), 0}, fn
            {{_idx, key, {:flow_step_continue, _key, attrs}}, local_idx}, {buckets, count} ->
              shard_idx = shard_for(ctx, key)
              {flow_put_shard_bucket(buckets, shard_idx, {local_idx, key, attrs}), count + 1}
          end)

        groups =
          flow_nonempty_shard_buckets(buckets, ctx.shard_count, fn shard_idx, entries ->
            group = Enum.reverse(entries)
            key = group |> hd() |> elem(1)
            local_indices = Enum.map(group, fn {idx, _key, _attrs} -> idx end)
            attrs_list = Enum.map(group, fn {_idx, _key, attrs} -> attrs end)

            command_attrs =
              %{records: attrs_list, independent: true}
              |> flow_stamp_shard(shard_idx)

            {key, local_indices, {:flow_step_continue_many, key, command_attrs}}
          end)

        keyed_commands = Enum.map(groups, fn {key, _indices, cmd} -> {key, cmd} end)
        group_results = batch_quorum_commands(ctx, keyed_commands)
        expand_flow_transition_batch_results(count, groups, group_results)
      end

      def flow_signal_batch(_ctx, []), do: []

      def flow_signal_batch(ctx, attrs_list) when is_list(attrs_list) do
        if ctx.name == :default do
          {valid, indexed_results} =
            attrs_list
            |> Enum.with_index()
            |> Enum.reduce({[], %{}}, fn
              {%{id: id} = attrs, idx}, {valid_acc, result_acc} when is_binary(id) ->
                key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

                if byte_size(key) > @max_key_size do
                  {valid_acc,
                   Map.put(
                     result_acc,
                     idx,
                     {:error, "ERR key too large (max #{@max_key_size} bytes)"}
                   )}
                else
                  {[{idx, key, {:flow_signal, key, attrs}} | valid_acc], result_acc}
                end

              {_attrs, idx}, {valid_acc, result_acc} ->
                {valid_acc,
                 Map.put(result_acc, idx, {:error, "ERR flow id must be a non-empty string"})}
            end)

          valid = Enum.reverse(valid)
          valid_results = flow_signal_batch_valid_results(ctx, valid)

          indexed_results =
            valid
            |> Enum.map(fn {idx, _key, _cmd} -> idx end)
            |> Enum.zip(valid_results)
            |> Enum.reduce(indexed_results, fn {idx, result}, acc -> Map.put(acc, idx, result) end)

          for idx <- 0..(length(attrs_list) - 1), do: Map.fetch!(indexed_results, idx)
        else
          attrs_list
          |> Enum.map(fn attrs ->
            key =
              Ferricstore.Flow.Keys.state_key(Map.get(attrs, :id), Map.get(attrs, :partition_key))

            raft_write(ctx, shard_for(ctx, key), key, {:flow_signal, key, attrs})
          end)
        end
      end

      defp flow_signal_batch_valid_results(_ctx, []), do: []

      defp flow_signal_batch_valid_results(ctx, valid) do
        {buckets, count} =
          valid
          |> Enum.with_index()
          |> Enum.reduce({flow_fixed_shard_buckets(ctx.shard_count), 0}, fn
            {{_idx, key, {:flow_signal, _key, attrs}}, local_idx}, {buckets, count} ->
              shard_idx = shard_for(ctx, key)
              {flow_put_shard_bucket(buckets, shard_idx, {local_idx, key, attrs}), count + 1}
          end)

        groups =
          flow_nonempty_shard_buckets(buckets, ctx.shard_count, fn shard_idx, entries ->
            group = Enum.reverse(entries)
            key = group |> hd() |> elem(1)
            local_indices = Enum.map(group, fn {idx, _key, _attrs} -> idx end)
            attrs_list = Enum.map(group, fn {_idx, _key, attrs} -> attrs end)

            command_attrs =
              %{records: attrs_list, independent: true}
              |> flow_stamp_shard(shard_idx)

            {key, local_indices, {:flow_signal_many, key, command_attrs}}
          end)

        keyed_commands = Enum.map(groups, fn {key, _indices, cmd} -> {key, cmd} end)
        group_results = batch_quorum_commands(ctx, keyed_commands)
        expand_flow_transition_batch_results(count, groups, group_results)
      end
    end
  end
end
