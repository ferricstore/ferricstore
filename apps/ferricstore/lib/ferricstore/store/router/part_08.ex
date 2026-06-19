defmodule Ferricstore.Store.Router.Part08 do
  @moduledoc false

  # Extracted from Router: expand_flow_transition_batch_results .. zpopmax
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

      defp expand_flow_transition_batch_results(count, groups, group_results) do
        results =
          group_results
          |> Enum.zip(groups)
          |> Enum.reduce(new_waraft_result_tuple(count), fn
            {results, {_key, indices, _cmd}}, acc when is_list(results) ->
              put_flow_transition_batch_results(indices, results, acc)

            {{:error, _reason} = error, {_key, indices, _cmd}}, acc ->
              Enum.reduce(indices, acc, fn idx, next -> put_elem(next, idx, error) end)

            {other, {_key, indices, _cmd}}, acc ->
              Enum.reduce(indices, acc, fn idx, next -> put_elem(next, idx, other) end)
          end)

        Tuple.to_list(results)
      end

      defp put_flow_transition_batch_results([], _results, acc), do: acc
      defp put_flow_transition_batch_results(_indices, [], acc), do: acc

      defp put_flow_transition_batch_results([index | indices], [result | results], acc) do
        put_flow_transition_batch_results(indices, results, put_elem(acc, index, result))
      end

      @doc false
      def flow_transition_many(ctx, partition_key, attrs_list)
          when is_binary(partition_key) and is_list(attrs_list) do
        key = Ferricstore.Flow.Keys.state_key("__transition_batch__", partition_key)

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)

          raft_write(
            ctx,
            idx,
            key,
            {:flow_transition_many, key, flow_transition_many_command_attrs(attrs_list)}
          )
        end
      end

      def flow_transition_many(ctx, nil, attrs_list) when is_list(attrs_list) do
        flow_many_by_shard(ctx, attrs_list, :flow_transition_many, "__transition_batch__")
      end

      @doc false
      def flow_retry(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          case flow_cross_terminal_keys(ctx, id, Map.get(attrs, :partition_key)) do
            {:ok, keys} ->
              flow_cross_shard_tx(ctx, keys, {:flow_cross_terminal, :retry, attrs})

            :same_or_none ->
              idx = shard_for(ctx, key)
              raft_write(ctx, idx, key, {:flow_retry, key, attrs})
          end
        end
      end

      @doc false
      def flow_retry_many(ctx, partition_key, attrs_list)
          when is_binary(partition_key) and is_list(attrs_list) do
        key = Ferricstore.Flow.Keys.state_key("__retry_batch__", partition_key)

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          case flow_cross_terminal_many_keys(ctx, attrs_list) do
            {:ok, keys} ->
              flow_cross_shard_tx(
                ctx,
                keys,
                {:flow_cross_terminal_many, :retry, %{records: attrs_list}}
              )

            :same_or_none ->
              idx = shard_for(ctx, key)

              raft_write(
                ctx,
                idx,
                key,
                {:flow_retry_many, key, %{records: attrs_list}}
              )
          end
        end
      end

      def flow_retry_many(ctx, nil, attrs_list) when is_list(attrs_list) do
        case flow_cross_terminal_many_keys(ctx, attrs_list) do
          {:ok, keys} ->
            flow_cross_shard_tx(
              ctx,
              keys,
              {:flow_cross_terminal_many, :retry, %{records: attrs_list}}
            )

          :same_or_none ->
            flow_many_by_shard(ctx, attrs_list, :flow_retry_many, "__retry_batch__")
        end
      end

      @doc false
      def flow_fail(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          case flow_cross_terminal_keys(ctx, id, Map.get(attrs, :partition_key)) do
            {:ok, keys} ->
              flow_cross_shard_tx(ctx, keys, {:flow_cross_terminal, :fail, attrs})

            :same_or_none ->
              idx = shard_for(ctx, key)
              raft_write(ctx, idx, key, {:flow_fail, key, attrs})
          end
        end
      end

      @doc false
      def flow_fail_many(ctx, partition_key, attrs_list)
          when is_binary(partition_key) and is_list(attrs_list) do
        key = Ferricstore.Flow.Keys.state_key("__fail_batch__", partition_key)

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          case flow_cross_terminal_many_keys(ctx, attrs_list) do
            {:ok, keys} ->
              flow_cross_shard_tx(
                ctx,
                keys,
                {:flow_cross_terminal_many, :fail, %{records: attrs_list}}
              )

            :same_or_none ->
              idx = shard_for(ctx, key)

              raft_write(
                ctx,
                idx,
                key,
                {:flow_fail_many, key, %{records: attrs_list}}
              )
          end
        end
      end

      def flow_fail_many(ctx, nil, attrs_list) when is_list(attrs_list) do
        case flow_cross_terminal_many_keys(ctx, attrs_list) do
          {:ok, keys} ->
            flow_cross_shard_tx(
              ctx,
              keys,
              {:flow_cross_terminal_many, :fail, %{records: attrs_list}}
            )

          :same_or_none ->
            flow_many_by_shard(ctx, attrs_list, :flow_fail_many, "__fail_batch__")
        end
      end

      @doc false
      def flow_cancel(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          case flow_cross_terminal_keys(ctx, id, Map.get(attrs, :partition_key)) do
            {:ok, keys} ->
              flow_cross_shard_tx(ctx, keys, {:flow_cross_terminal, :cancel, attrs})

            :same_or_none ->
              idx = shard_for(ctx, key)
              raft_write(ctx, idx, key, {:flow_cancel, key, attrs})
          end
        end
      end

      defp flow_cross_terminal_keys(ctx, id, partition_key) do
        case flow_terminal_keys(ctx, id, partition_key) do
          {:ok, keys} ->
            if flow_keys_cross_shard?(ctx, keys), do: {:ok, keys}, else: :same_or_none

          :missing ->
            :same_or_none
        end
      end

      defp flow_cross_terminal_many_keys(ctx, attrs_list) do
        entries = flow_terminal_many_key_entries(ctx, attrs_list)

        if Enum.any?(entries, &flow_terminal_key_entry_cross_shard?(ctx, &1)) do
          keys =
            entries
            |> Enum.flat_map(fn
              {:ok, keys} -> keys
              :missing -> []
            end)
            |> Enum.uniq()

          {:ok, keys}
        else
          :same_or_none
        end
      end

      defp flow_terminal_many_key_entries(ctx, attrs_list) do
        keyed_attrs =
          Enum.map(attrs_list, fn
            %{id: id} = attrs when is_binary(id) and id != "" ->
              partition_key = Map.get(attrs, :partition_key)
              {Ferricstore.Flow.Keys.state_key(id, partition_key), id, partition_key}

            _attrs ->
              :missing
          end)

        keys =
          keyed_attrs
          |> Enum.flat_map(fn
            {key, _id, _partition_key} -> [key]
            :missing -> []
          end)

        values = flow_terminal_many_values(ctx, keys)
        value_by_key = Map.new(Enum.zip(keys, values))
        noop_by_key = Map.new(Enum.zip(keys, flow_terminal_many_noop_flags(values)))

        Enum.map(keyed_attrs, fn
          {child_key, _id, _partition_key} ->
            case Map.get(value_by_key, child_key) do
              value when is_binary(value) ->
                if Map.get(noop_by_key, child_key) do
                  {:ok, [child_key]}
                else
                  flow_terminal_key_entry_decode(child_key, value)
                end

              _other ->
                :missing
            end

          :missing ->
            :missing
        end)
      end

      defp flow_terminal_many_noop_flags(values) do
        binaries = for value <- values, is_binary(value), do: value

        flags =
          case binaries do
            [] -> []
            [_ | _] -> Ferricstore.Bitcask.NIF.flow_records_terminal_after_noop(binaries)
          end

        {result, _remaining} =
          Enum.map_reduce(values, flags, fn
            value, [flag | remaining] when is_binary(value) ->
              {flag == true, remaining}

            value, remaining when is_binary(value) ->
              {false, remaining}

            _value, remaining ->
              {false, remaining}
          end)

        result
      rescue
        _ -> List.duplicate(false, length(values))
      end

      defp flow_terminal_key_entry_decode(child_key, value) do
        case Ferricstore.Flow.decode_record(value) do
          %{partition_key: _partition_key} = record ->
            {:ok,
             [child_key]
             |> flow_maybe_add_parent_key(record)
             |> flow_add_child_group_keys(record)
             |> Enum.uniq()}

          _other ->
            :missing
        end
      rescue
        _ -> :missing
      end

      defp flow_terminal_many_values(_ctx, []), do: []

      defp flow_terminal_many_values(ctx, keys) do
        if hook = Process.get(:ferricstore_flow_terminal_many_values_hook) do
          hook.(keys)
        end

        values = batch_get(ctx, keys)
        missing_keys = for {key, nil} <- Enum.zip(keys, values), do: key
        missing_values = flow_batch_get_lmdb(ctx, missing_keys, :lagged)

        {merged, []} =
          Enum.map_reduce(values, missing_values, fn
            value, remaining when is_binary(value) -> {value, remaining}
            nil, [value | remaining] -> {value, remaining}
            nil, [] -> {nil, []}
            _other, remaining -> {nil, remaining}
          end)

        merged
      end

      defp flow_terminal_key_entry_cross_shard?(ctx, {:ok, keys}),
        do: flow_keys_cross_shard?(ctx, keys)

      defp flow_terminal_key_entry_cross_shard?(_ctx, _entry), do: false

      defp flow_terminal_keys(ctx, id, partition_key) do
        child_key = Ferricstore.Flow.Keys.state_key(id, partition_key)

        case flow_get(ctx, id, partition_key) do
          value when is_binary(value) ->
            flow_terminal_key_entry_decode(child_key, value)

          _other ->
            :missing
        end
      rescue
        _ -> :missing
      end

      defp flow_maybe_add_parent_key(keys, record) do
        parent_id = Map.get(record, :parent_flow_id)

        parent_partition =
          Map.get(record, :parent_partition_key) || Map.get(record, :partition_key)

        if is_binary(parent_id) and parent_id != "" do
          [Ferricstore.Flow.Keys.state_key(parent_id, parent_partition) | keys]
        else
          keys
        end
      end

      defp flow_add_child_group_keys(keys, record) do
        partition_key = Map.get(record, :partition_key)

        record
        |> Map.get(:child_groups, %{})
        |> Enum.flat_map(fn {_group_id, group} ->
          child_partitions = Map.get(group, "child_partitions", %{})

          group
          |> Map.get("children", %{})
          |> Enum.flat_map(fn
            {child_id, "running"} ->
              [
                Ferricstore.Flow.Keys.state_key(
                  child_id,
                  Map.get(child_partitions, child_id, partition_key)
                )
              ]

            _other ->
              []
          end)
        end)
        |> Kernel.++(keys)
      end

      @doc false
      def flow_cancel_many(ctx, partition_key, attrs_list)
          when is_binary(partition_key) and is_list(attrs_list) do
        key = Ferricstore.Flow.Keys.state_key("__cancel_batch__", partition_key)

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          case flow_cross_terminal_many_keys(ctx, attrs_list) do
            {:ok, keys} ->
              flow_cross_shard_tx(
                ctx,
                keys,
                {:flow_cross_terminal_many, :cancel, %{records: attrs_list}}
              )

            :same_or_none ->
              idx = shard_for(ctx, key)

              raft_write(
                ctx,
                idx,
                key,
                {:flow_cancel_many, key, %{records: attrs_list}}
              )
          end
        end
      end

      def flow_cancel_many(ctx, nil, attrs_list) when is_list(attrs_list) do
        case flow_cross_terminal_many_keys(ctx, attrs_list) do
          {:ok, keys} ->
            flow_cross_shard_tx(
              ctx,
              keys,
              {:flow_cross_terminal_many, :cancel, %{records: attrs_list}}
            )

          :same_or_none ->
            flow_many_by_shard(ctx, attrs_list, :flow_cancel_many, "__cancel_batch__")
        end
      end

      @doc false
      def flow_create_pipeline_batch(_ctx, []), do: []

      def flow_create_pipeline_batch(ctx, attrs_list) when is_list(attrs_list) do
        {buckets, count, rejected} =
          Enum.reduce(attrs_list, {flow_fixed_shard_buckets(ctx.shard_count), 0, %{}}, fn
            %{id: id} = attrs, {buckets, index, rejected} ->
              key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

              if flow_create_admission_rejected?(ctx, key) do
                {buckets, index + 1, Map.put(rejected, index, flow_create_overloaded_error())}
              else
                shard_idx = shard_for(ctx, key)

                {flow_put_shard_bucket(buckets, shard_idx, {index, key, attrs}), index + 1,
                 rejected}
              end
          end)

        groups =
          flow_nonempty_shard_buckets(buckets, ctx.shard_count, fn shard_idx, entries ->
            entries = Enum.reverse(entries)
            {indices, keys, attrs} = flow_create_pipeline_entries(entries, [], [], [])
            route_key = List.first(keys)

            {shard_idx, route_key, indices,
             {:flow_create_pipeline_batch, route_key,
              flow_stamp_shard(%{records: attrs}, shard_idx)}}
          end)

        keyed_commands = Enum.map(groups, fn {_shard_idx, key, _indices, cmd} -> {key, cmd} end)
        group_results = batch_quorum_commands(ctx, keyed_commands)

        results =
          groups
          |> Enum.zip(group_results)
          |> Enum.reduce(flow_result_tuple(count, rejected), fn {{_shard_idx, _route_key, indices,
                                                                  _cmd}, result},
                                                                results ->
            group_results =
              case result do
                result when is_list(result) and length(result) == length(indices) -> result
                result -> List.duplicate(result, length(indices))
              end

            indices
            |> Enum.zip(group_results)
            |> Enum.reduce(results, fn {index, result}, results ->
              put_elem(results, index, result)
            end)
          end)

        Tuple.to_list(results)
      end

      @doc false
      def flow_start_and_claim_pipeline_batch(_ctx, []), do: []

      def flow_start_and_claim_pipeline_batch(ctx, attrs_list) when is_list(attrs_list) do
        {buckets, count, rejected} =
          Enum.reduce(attrs_list, {flow_fixed_shard_buckets(ctx.shard_count), 0, %{}}, fn
            %{id: id} = attrs, {buckets, index, rejected} ->
              key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

              if flow_create_admission_rejected?(ctx, key) do
                {buckets, index + 1, Map.put(rejected, index, flow_create_overloaded_error())}
              else
                shard_idx = shard_for(ctx, key)

                {flow_put_shard_bucket(buckets, shard_idx, {index, key, attrs}), index + 1,
                 rejected}
              end
          end)

        groups =
          flow_nonempty_shard_buckets(buckets, ctx.shard_count, fn shard_idx, entries ->
            entries = Enum.reverse(entries)
            {indices, keys, attrs} = flow_create_pipeline_entries(entries, [], [], [])
            route_key = List.first(keys)

            {shard_idx, route_key, indices,
             {:flow_start_and_claim_pipeline_batch, route_key,
              flow_stamp_shard(%{records: attrs}, shard_idx)}}
          end)

        keyed_commands = Enum.map(groups, fn {_shard_idx, key, _indices, cmd} -> {key, cmd} end)
        group_results = batch_quorum_commands(ctx, keyed_commands)

        results =
          groups
          |> Enum.zip(group_results)
          |> Enum.reduce(flow_result_tuple(count, rejected), fn {{_shard_idx, _route_key, indices,
                                                                  _cmd}, result},
                                                                results ->
            group_results =
              case result do
                result when is_list(result) and length(result) == length(indices) -> result
                result -> List.duplicate(result, length(indices))
              end

            indices
            |> Enum.zip(group_results)
            |> Enum.reduce(results, fn {index, result}, results ->
              put_elem(results, index, result)
            end)
          end)

        Tuple.to_list(results)
      end

      @doc false
      def flow_named_value_put_pipeline_batch(_ctx, []), do: []

      def flow_named_value_put_pipeline_batch(ctx, attrs_list) when is_list(attrs_list) do
        {buckets, count, rejected} =
          Enum.reduce(attrs_list, {flow_fixed_shard_buckets(ctx.shard_count), 0, %{}}, fn
            %{id: id} = attrs, {buckets, index, rejected} when is_binary(id) ->
              key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

              if byte_size(key) > @max_key_size do
                {buckets, index + 1,
                 Map.put(
                   rejected,
                   index,
                   {:error, "ERR key too large (max #{@max_key_size} bytes)"}
                 )}
              else
                shard_idx = shard_for(ctx, key)

                {flow_put_shard_bucket(buckets, shard_idx, {index, key, attrs}), index + 1,
                 rejected}
              end
          end)

        groups =
          flow_nonempty_shard_buckets(buckets, ctx.shard_count, fn shard_idx, entries ->
            entries = Enum.reverse(entries)
            {indices, keys, attrs} = flow_create_pipeline_entries(entries, [], [], [])
            route_key = List.first(keys)

            {shard_idx, route_key, indices,
             {:flow_named_value_put_pipeline_batch, route_key,
              flow_stamp_shard(%{records: attrs}, shard_idx)}}
          end)

        keyed_commands = Enum.map(groups, fn {_shard_idx, key, _indices, cmd} -> {key, cmd} end)
        group_results = batch_quorum_commands(ctx, keyed_commands)

        results =
          groups
          |> Enum.zip(group_results)
          |> Enum.reduce(flow_result_tuple(count, rejected), fn {{_shard_idx, _route_key, indices,
                                                                  _cmd}, result},
                                                                results ->
            group_results =
              case result do
                result when is_list(result) and length(result) == length(indices) -> result
                result -> List.duplicate(result, length(indices))
              end

            indices
            |> Enum.zip(group_results)
            |> Enum.reduce(results, fn {index, result}, results ->
              put_elem(results, index, result)
            end)
          end)

        Tuple.to_list(results)
      end

      @doc false
      def flow_create_pipeline_batch_ok_on_success(_ctx, []), do: :ok

      def flow_create_pipeline_batch_ok_on_success(ctx, attrs_list) when is_list(attrs_list) do
        {buckets, count, rejected} =
          Enum.reduce(attrs_list, {flow_fixed_shard_buckets(ctx.shard_count), 0, %{}}, fn
            %{id: id} = attrs, {buckets, index, rejected} ->
              key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

              if flow_create_admission_rejected?(ctx, key) do
                {buckets, index + 1, Map.put(rejected, index, flow_create_overloaded_error())}
              else
                shard_idx = shard_for(ctx, key)

                {flow_put_shard_bucket(buckets, shard_idx, {index, key, attrs}), index + 1,
                 rejected}
              end
          end)

        groups =
          flow_nonempty_shard_buckets(buckets, ctx.shard_count, fn shard_idx, entries ->
            entries = Enum.reverse(entries)
            {indices, keys, attrs} = flow_create_pipeline_entries(entries, [], [], [])
            route_key = List.first(keys)

            {shard_idx, route_key, indices,
             {:flow_create_pipeline_batch, route_key,
              flow_stamp_shard(%{records: attrs}, shard_idx)}}
          end)

        keyed_commands = Enum.map(groups, fn {_shard_idx, key, _indices, cmd} -> {key, cmd} end)
        group_results = batch_quorum_commands(ctx, keyed_commands)

        if map_size(rejected) == 0 and Enum.all?(group_results, &flow_create_group_success?/1) do
          :ok
        else
          results =
            groups
            |> Enum.zip(group_results)
            |> Enum.reduce(flow_result_tuple(count, rejected), fn {{_shard_idx, _route_key,
                                                                    indices, _cmd}, result},
                                                                  results ->
              group_results =
                case result do
                  result when is_list(result) and length(result) == length(indices) -> result
                  result -> List.duplicate(result, length(indices))
                end

              indices
              |> Enum.zip(group_results)
              |> Enum.reduce(results, fn {index, result}, results ->
                put_elem(results, index, result)
              end)
            end)

          Tuple.to_list(results)
        end
      end

      defp flow_create_group_success?(:ok), do: true

      defp flow_create_group_success?(results) when is_list(results),
        do: Enum.all?(results, &(&1 == :ok))

      defp flow_create_group_success?(_result), do: false

      defp flow_create_pipeline_entries([], indices, keys, attrs) do
        {Enum.reverse(indices), Enum.reverse(keys), Enum.reverse(attrs)}
      end

      defp flow_create_pipeline_entries([{index, key, record_attrs} | rest], indices, keys, attrs) do
        flow_create_pipeline_entries(rest, [index | indices], [key | keys], [record_attrs | attrs])
      end

      defp flow_fixed_shard_buckets(shard_count) when is_integer(shard_count) and shard_count > 0,
        do: :erlang.make_tuple(shard_count, [])

      defp flow_put_shard_bucket(buckets, shard_idx, entry) do
        put_elem(buckets, shard_idx, [entry | elem(buckets, shard_idx)])
      end

      defp flow_nonempty_shard_buckets(buckets, shard_count, fun) do
        0..(shard_count - 1)
        |> Enum.reduce([], fn shard_idx, acc ->
          case elem(buckets, shard_idx) do
            [] -> acc
            entries -> [fun.(shard_idx, entries) | acc]
          end
        end)
        |> Enum.reverse()
      end

      defp flow_result_tuple(count, rejected \\ %{}) when is_integer(count) and count >= 0 do
        base = :erlang.make_tuple(count, ErrorReasons.write_timeout_unknown())

        Enum.reduce(rejected, base, fn {index, result}, acc ->
          put_elem(acc, index, result)
        end)
      end

      @doc false
      def flow_terminal_command_batch(_ctx, []), do: []

      def flow_terminal_command_batch(ctx, commands) when is_list(commands) do
        case flow_terminal_pipeline_batch(ctx, commands) do
          {:ok, results} ->
            results

          :fallback ->
            commands
            |> flow_terminal_command_batch(ctx, [], [])
            |> Enum.reverse()
        end
      end

      @doc false
      def flow_terminal_command_batch_independent(_ctx, []), do: []

      def flow_terminal_command_batch_independent(ctx, commands) when is_list(commands) do
        case flow_terminal_pipeline_independent_batch(ctx, commands) do
          {:ok, results} ->
            results

          :fallback ->
            commands
            |> flow_terminal_command_batch_independent(ctx, [], [])
            |> Enum.reverse()
        end
      end

      defp flow_terminal_pipeline_batch(ctx, commands) do
        with {:ok, op, attrs_list} <- flow_terminal_homogeneous_attrs(commands),
             :ok <- flow_terminal_pipeline_keys_valid?(attrs_list),
             :same_or_none <- flow_cross_terminal_many_keys(ctx, attrs_list) do
          {:ok, flow_terminal_pipeline_same_shard(ctx, op, attrs_list)}
        else
          _other -> :fallback
        end
      end

      defp flow_terminal_pipeline_independent_batch(ctx, commands) do
        with {:ok, op, attrs_list} <- flow_terminal_homogeneous_attrs(commands),
             :ok <- flow_terminal_pipeline_keys_valid?(attrs_list) do
          {:ok, flow_terminal_pipeline_same_shard(ctx, op, attrs_list)}
        else
          _other -> :fallback
        end
      end

      defp flow_terminal_homogeneous_attrs([{op, %{id: id} = attrs} | rest])
           when op in [:complete, :retry, :fail, :cancel] and is_binary(id) and id != "" do
        flow_terminal_homogeneous_attrs(rest, op, [attrs])
      end

      defp flow_terminal_homogeneous_attrs(_commands), do: :fallback

      defp flow_terminal_homogeneous_attrs([], op, acc), do: {:ok, op, Enum.reverse(acc)}

      defp flow_terminal_homogeneous_attrs([{op, %{id: id} = attrs} | rest], op, acc)
           when op in [:complete, :retry, :fail, :cancel] and is_binary(id) and id != "" do
        flow_terminal_homogeneous_attrs(rest, op, [attrs | acc])
      end

      defp flow_terminal_homogeneous_attrs(_commands, _op, _acc), do: :fallback

      defp flow_terminal_pipeline_keys_valid?(attrs_list) do
        if Enum.all?(attrs_list, fn %{id: id} = attrs ->
             key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))
             byte_size(key) <= @max_key_size
           end) do
          :ok
        else
          :fallback
        end
      end

      defp flow_terminal_pipeline_same_shard(ctx, op, attrs_list) do
        {buckets, count} =
          attrs_list
          |> Enum.with_index()
          |> Enum.reduce({flow_fixed_shard_buckets(ctx.shard_count), 0}, fn {%{id: id} = attrs,
                                                                             index},
                                                                            {buckets, count} ->
            key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))
            shard_idx = shard_for(ctx, key)

            {flow_put_shard_bucket(buckets, shard_idx, {index, key, attrs}),
             max(count, index + 1)}
          end)

        groups =
          buckets
          |> flow_nonempty_shard_buckets(ctx.shard_count, fn shard_idx, entries ->
            {shard_idx, entries}
          end)
          |> Enum.map(fn {shard_idx, entries} ->
            entries = Enum.reverse(entries)
            {indices, keys, attrs} = flow_terminal_pipeline_entries(entries, [], [], [])
            route_key = List.first(keys)

            {shard_idx, route_key, indices,
             {:flow_terminal_pipeline_batch, op, route_key,
              flow_stamp_shard(%{records: attrs}, shard_idx)}}
          end)

        keyed_commands = Enum.map(groups, fn {_shard_idx, key, _indices, cmd} -> {key, cmd} end)
        group_results = batch_quorum_commands(ctx, keyed_commands)

        results =
          groups
          |> Enum.zip(group_results)
          |> Enum.reduce(flow_result_tuple(count), fn {{_shard_idx, _route_key, indices, _cmd},
                                                       result},
                                                      acc ->
            group_results =
              case result do
                result when is_list(result) and length(result) == length(indices) -> result
                result -> List.duplicate(result, length(indices))
              end

            indices
            |> Enum.zip(group_results)
            |> Enum.reduce(acc, fn {index, result}, results_acc ->
              put_elem(results_acc, index, result)
            end)
          end)

        Tuple.to_list(results)
      end

      defp flow_terminal_pipeline_entries([], indices, keys, attrs) do
        {Enum.reverse(indices), Enum.reverse(keys), Enum.reverse(attrs)}
      end

      defp flow_terminal_pipeline_entries(
             [{index, key, record_attrs} | rest],
             indices,
             keys,
             attrs
           ) do
        flow_terminal_pipeline_entries(rest, [index | indices], [key | keys], [
          record_attrs | attrs
        ])
      end

      defp flow_terminal_command_batch([], ctx, batch_acc, result_acc) do
        flow_terminal_flush_batch(ctx, batch_acc, result_acc)
      end

      defp flow_terminal_command_batch(
             [{op, %{id: id} = attrs} | rest],
             ctx,
             batch_acc,
             result_acc
           )
           when op in [:complete, :retry, :fail, :cancel] and is_binary(id) do
        case flow_terminal_batch_command(ctx, op, attrs) do
          {:batch, key, command} ->
            flow_terminal_command_batch(rest, ctx, [{key, command} | batch_acc], result_acc)

          {:direct, keys, command} ->
            result_acc = flow_terminal_flush_batch(ctx, batch_acc, result_acc)
            result = flow_cross_shard_tx(ctx, keys, command)
            flow_terminal_command_batch(rest, ctx, [], [result | result_acc])

          {:result, result} ->
            result_acc = flow_terminal_flush_batch(ctx, batch_acc, result_acc)
            flow_terminal_command_batch(rest, ctx, [], [result | result_acc])
        end
      end

      defp flow_terminal_command_batch([_invalid | rest], ctx, batch_acc, result_acc) do
        result_acc = flow_terminal_flush_batch(ctx, batch_acc, result_acc)

        flow_terminal_command_batch(
          rest,
          ctx,
          [],
          [{:error, "ERR flow id must be a non-empty string"} | result_acc]
        )
      end

      defp flow_terminal_flush_batch(_ctx, [], result_acc), do: result_acc

      defp flow_terminal_flush_batch(ctx, batch_acc, result_acc) do
        results = batch_quorum_commands(ctx, Enum.reverse(batch_acc), nil)

        Enum.reverse(results) ++ result_acc
      end

      defp flow_terminal_command_batch_independent([], ctx, batch_acc, result_acc) do
        flow_terminal_flush_batch(ctx, batch_acc, result_acc)
      end

      defp flow_terminal_command_batch_independent(
             [{op, %{id: id} = attrs} | rest],
             ctx,
             batch_acc,
             result_acc
           )
           when op in [:complete, :retry, :fail, :cancel] and is_binary(id) do
        case flow_terminal_batch_command_independent(op, attrs) do
          {:batch, key, command} ->
            flow_terminal_command_batch_independent(
              rest,
              ctx,
              [{key, command} | batch_acc],
              result_acc
            )

          {:result, result} ->
            result_acc = flow_terminal_flush_batch(ctx, batch_acc, result_acc)
            flow_terminal_command_batch_independent(rest, ctx, [], [result | result_acc])
        end
      end

      defp flow_terminal_command_batch_independent([_invalid | rest], ctx, batch_acc, result_acc) do
        result_acc = flow_terminal_flush_batch(ctx, batch_acc, result_acc)

        flow_terminal_command_batch_independent(
          rest,
          ctx,
          [],
          [{:error, "ERR flow id must be a non-empty string"} | result_acc]
        )
      end

      defp flow_terminal_batch_command_independent(op, %{id: id} = attrs)
           when op in [:complete, :retry, :fail, :cancel] do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:result, {:error, "ERR key too large (max #{@max_key_size} bytes)"}}
        else
          {:batch, key, flow_terminal_raft_command(op, key, attrs)}
        end
      end

      defp flow_terminal_batch_command(ctx, op, %{id: id} = attrs) do
        partition_key = Map.get(attrs, :partition_key)
        key = Ferricstore.Flow.Keys.state_key(id, partition_key)

        if byte_size(key) > @max_key_size do
          {:result, {:error, "ERR key too large (max #{@max_key_size} bytes)"}}
        else
          case flow_cross_terminal_keys(ctx, id, partition_key) do
            {:ok, keys} ->
              {:direct, keys, {:flow_cross_terminal, op, attrs}}

            :same_or_none ->
              {:batch, key, flow_terminal_raft_command(op, key, attrs)}
          end
        end
      end

      defp flow_terminal_raft_command(:complete, key, attrs), do: {:flow_complete, key, attrs}
      defp flow_terminal_raft_command(:retry, key, attrs), do: {:flow_retry, key, attrs}
      defp flow_terminal_raft_command(:fail, key, attrs), do: {:flow_fail, key, attrs}
      defp flow_terminal_raft_command(:cancel, key, attrs), do: {:flow_cancel, key, attrs}

      @doc false
      def flow_retention_cleanup(ctx, attrs) when is_map(attrs) do
        0..(ctx.shard_count - 1)
        |> Enum.reduce_while({:ok, %{flows: 0, history: 0, values: 0}}, fn idx, {:ok, acc} ->
          key = "__flow_retention_cleanup__:#{idx}"

          case raft_write(ctx, idx, key, {:flow_retention_cleanup, key, attrs}) do
            {:ok, result} when is_map(result) ->
              {:cont, {:ok, merge_flow_cleanup_counts(acc, result)}}

            {:error, _reason} = error ->
              {:halt, error}

            _other ->
              {:halt, {:error, "ERR flow retention cleanup failed"}}
          end
        end)
      end

      defp merge_flow_cleanup_counts(left, right) do
        %{
          flows: Map.get(left, :flows, 0) + Map.get(right, :flows, 0),
          history: Map.get(left, :history, 0) + Map.get(right, :history, 0),
          values: Map.get(left, :values, 0) + Map.get(right, :values, 0)
        }
      end

      @doc false
      def flow_rewind(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:flow_rewind, key, attrs})
        end
      end

      @doc false
      def pfadd(ctx, key, elements) when is_binary(key) and is_list(elements) do
        forced_single_key_quorum(ctx, key, {:pfadd, key, elements})
      end

      @doc false
      def pfmerge(ctx, dest_key, source_keys)
          when is_binary(dest_key) and is_list(source_keys) and source_keys != [] do
        with {:ok, [_dest_sketch | source_sketches]} <-
               hll_read_sketches(ctx, [dest_key | source_keys]) do
          forced_single_key_quorum(
            ctx,
            dest_key,
            {:pfmerge, dest_key, source_keys, source_sketches}
          )
        end
      end

      def pfmerge(_ctx, _dest_key, _source_keys),
        do: {:error, "ERR wrong number of arguments for 'pfmerge' command"}

      @doc false
      def spop(ctx, key, count) when is_binary(key) and (is_nil(count) or is_integer(count)) do
        forced_single_key_quorum(ctx, key, {:spop, key, count})
      end

      @doc false
      def zpopmin(ctx, key, count) when is_binary(key) and is_integer(count) do
        forced_single_key_quorum(ctx, key, {:zpop, key, count, :min})
      end

      @doc false
      def zpopmax(ctx, key, count) when is_binary(key) and is_integer(count) do
        forced_single_key_quorum(ctx, key, {:zpop, key, count, :max})
      end
    end
  end
end
