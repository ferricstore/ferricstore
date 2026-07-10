defmodule Ferricstore.Store.Router.Part06 do
  @moduledoc false

  # Extracted from Router: forward_batch_to_leader .. expand_flow_many_results
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
      # Forward a final put_batch to the leader's node. Used by batch_quorum_put when
      # the local Batcher rejects with :not_leader. The entries are already in the
      # optimized `{key, value, expire_at_ms}` shape, so forwarded writes keep the
      # same shape instead of rebuilding old per-key command tuples.
      defp forward_batch_to_leader(_ctx, leader_node, _shard_idx, entries)
           when leader_node == node() do
        Enum.map(entries, fn _ -> {:error, "ERR not leader, election in progress"} end)
      end

      defp forward_batch_to_leader(_ctx, leader_node, shard_idx, entries) do
        try do
          remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)

          leader_results =
            :erpc.call(
              leader_node,
              __MODULE__,
              :__forwarded_batch_quorum_put_entries__,
              [remote_ctx, entries, node()],
              10_000
            )

          unwrap_forwarded_batch_results(shard_idx, leader_results)
        catch
          _, reason ->
            require Logger
            Logger.warning("batch forward to #{inspect(leader_node)} failed: #{inspect(reason)}")
            __forward_batch_failure_results__(reason, length(entries))
        end
      end

      defp forward_batch_commands_to_leader(_ctx, leader_node, _shard_idx, keyed_commands)
           when leader_node == node() do
        Enum.map(keyed_commands, fn _ -> {:error, "ERR not leader, election in progress"} end)
      end

      defp forward_batch_commands_to_leader(_ctx, leader_node, shard_idx, keyed_commands) do
        try do
          remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)

          leader_results =
            :erpc.call(
              leader_node,
              __MODULE__,
              :__forwarded_batch_quorum_commands__,
              [remote_ctx, keyed_commands, node()],
              10_000
            )

          unwrap_forwarded_batch_results(shard_idx, leader_results)
        catch
          _, reason ->
            require Logger

            Logger.warning(
              "batch command forward to #{inspect(leader_node)} failed: #{inspect(reason)}"
            )

            __forward_batch_failure_results__(reason, length(keyed_commands))
        end
      end

      defp forward_delete_batch_to_leader(_ctx, leader_node, _shard_idx, keys)
           when leader_node == node() do
        Enum.map(keys, fn _ -> {:error, "ERR not leader, election in progress"} end)
      end

      defp forward_delete_batch_to_leader(_ctx, leader_node, shard_idx, keys) do
        try do
          remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)

          leader_results =
            :erpc.call(
              leader_node,
              __MODULE__,
              :__forwarded_batch_quorum_delete__,
              [remote_ctx, keys, node()],
              10_000
            )

          unwrap_forwarded_batch_results(shard_idx, leader_results)
        catch
          _, reason ->
            require Logger

            Logger.warning(
              "delete batch forward to #{inspect(leader_node)} failed: #{inspect(reason)}"
            )

            __forward_batch_failure_results__(reason, length(keys))
        end
      end

      @doc false
      def __forwarded_batch_quorum_put__(ctx, kv_pairs, origin_node) do
        batch_quorum_put(ctx, kv_pairs, origin_node)
      end

      @doc false
      def __forwarded_batch_quorum_put_entries__(ctx, entries, origin_node) do
        do_batch_quorum_put_entries(ctx, entries, origin_node)
      end

      @doc false
      def __forwarded_batch_quorum_delete__(ctx, keys, origin_node) do
        batch_quorum_delete(ctx, keys, origin_node)
      end

      @doc false
      def __forwarded_batch_quorum_commands__(ctx, keyed_commands, origin_node) do
        batch_quorum_commands(ctx, keyed_commands, origin_node)
      end

      defp unwrap_forwarded_batch_results(shard_idx, results) when is_list(results) do
        Enum.map(results, &barrier_forwarded_result(shard_idx, &1, 5_000))
      end

      defp unwrap_forwarded_batch_results(_shard_idx, other), do: other

      @doc """
      Stores `key` with `value`. `expire_at_ms` is an absolute Unix-epoch
      timestamp in milliseconds; pass `0` for no expiry.
      """
      @spec put(FerricStore.Instance.t(), binary(), binary(), non_neg_integer()) ::
              :ok | {:error, binary()}
      def put(ctx, key, value, expire_at_ms \\ 0) do
        cond do
          byte_size(key) > @max_key_size ->
            {:error, "ERR key too large (max #{@max_key_size} bytes)"}

          is_binary(value) and byte_size(value) >= @max_value_size ->
            {:error, "ERR value too large (max #{@max_value_size} bytes)"}

          true ->
            case check_keydir_full(ctx, key) do
              :ok ->
                idx = shard_for(ctx, key)
                raft_write(ctx, idx, key, {:put, key, value, expire_at_ms})

              {:error, _} = err ->
                err
            end
        end
      end

      @doc false
      def flow_policy_put_all(ctx, key, value, expire_at_ms) do
        cond do
          byte_size(key) > @max_key_size ->
            {:error, "ERR key too large (max #{@max_key_size} bytes)"}

          is_binary(value) and byte_size(value) >= @max_value_size ->
            {:error, "ERR value too large (max #{@max_value_size} bytes)"}

          true ->
            case check_keydir_full(ctx, key) do
              :ok ->
                anchor_idx = 0
                command = flow_policy_put_all_command(ctx, key, value, expire_at_ms)

                ctx
                |> raft_write(anchor_idx, "f:{flow-policy}:tx", command)
                |> flow_policy_put_all_result(ctx, anchor_idx)

              {:error, _} = err ->
                err
            end
        end
      end

      defp flow_policy_put_all_command(ctx, key, value, expire_at_ms) do
        shard_batches =
          0..(ctx.shard_count - 1)
          |> Enum.map(fn shard_idx ->
            entry = {:flow_cross_policy_put, shard_idx, key, value, expire_at_ms}
            {shard_idx, [{shard_idx, entry}], nil}
          end)

        {:cross_shard_tx, shard_batches}
      end

      defp flow_policy_put_all_result(results, ctx, anchor_idx) when is_map(results) do
        expected_shards = Enum.to_list(0..(ctx.shard_count - 1))
        exact_shards? = results |> Map.keys() |> Enum.sort() == expected_shards

        if exact_shards? and Enum.all?(expected_shards, &(Map.get(results, &1) == [:ok])) do
          Enum.each(expected_shards, fn
            ^anchor_idx -> :ok
            shard_idx -> bump_write_version(ctx, shard_idx)
          end)

          :ok
        else
          {:error, "ERR flow policy transaction returned incomplete shard results"}
        end
      end

      defp flow_policy_put_all_result({:error, _reason} = error, _ctx, _anchor_idx), do: error

      defp flow_policy_put_all_result(_other, _ctx, _anchor_idx),
        do: {:error, "ERR flow policy transaction failed"}

      @doc false
      def flow_get(ctx, id, partition_key) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, partition_key)
        now_ms = CommandTime.now_ms()

        if flow_state_expired_or_deleted?(ctx, key, now_ms) do
          nil
        else
          case Stats.with_cache_tracking_disabled(fn -> get(ctx, key) end) do
            nil ->
              if flow_state_expired_or_deleted?(ctx, key, CommandTime.now_ms()),
                do: nil,
                else: flow_get_lmdb(ctx, key, :lagged)

            value when is_binary(value) ->
              if flow_terminal_record_expired?(value, CommandTime.now_ms()), do: nil, else: value

            value ->
              value
          end
        end
      end

      @doc false
      def flow_batch_get(ctx, ids, partition_key) when is_list(ids) do
        keys = Enum.map(ids, &Ferricstore.Flow.Keys.state_key(&1, partition_key))
        now_ms = CommandTime.now_ms()

        blocked_keys = flow_blocked_state_keys(ctx, keys, now_ms)

        values =
          Stats.with_cache_tracking_disabled(fn ->
            batch_get(ctx, keys)
          end)

        missing_keys =
          for {key, nil} <- Enum.zip(keys, values),
              not flow_state_key_blocked?(blocked_keys, key),
              not flow_state_expired_or_deleted?(ctx, key, CommandTime.now_ms()),
              do: key

        missing_values = flow_batch_get_lmdb(ctx, missing_keys, :lagged)

        {merged, []} =
          keys
          |> Enum.zip(values)
          |> Enum.map_reduce(missing_values, fn {key, value}, remaining ->
            cond do
              flow_state_key_blocked?(blocked_keys, key) ->
                {nil, remaining}

              is_binary(value) ->
                if flow_terminal_record_expired?(value, CommandTime.now_ms()),
                  do: {nil, remaining},
                  else: {value, remaining}

              is_nil(value) ->
                case remaining do
                  [cold_value | rest] -> {cold_value, rest}
                  [] -> {nil, []}
                end

              true ->
                {nil, remaining}
            end
          end)

        merged
      end

      defp flow_blocked_state_keys(ctx, keys, now_ms) do
        case Enum.reduce(keys, [], fn key, acc ->
               if flow_state_expired_or_deleted?(ctx, key, now_ms), do: [key | acc], else: acc
             end) do
          [] -> :none
          blocked -> MapSet.new(blocked)
        end
      end

      defp flow_state_key_blocked?(:none, _key), do: false
      defp flow_state_key_blocked?(blocked_keys, key), do: MapSet.member?(blocked_keys, key)

      @doc false
      def flow_lmdb_batch_get_state_keys(ctx, state_keys) when is_list(state_keys) do
        flow_batch_get_lmdb(ctx, state_keys, Ferricstore.Flow.LMDB.mode())
      end

      defp flow_batch_get_lmdb(_ctx, [], _mode), do: []

      defp flow_batch_get_lmdb(ctx, keys, mode) do
        now_ms = CommandTime.now_ms()

        values_by_index =
          keys
          |> Enum.with_index()
          |> Enum.group_by(fn {key, _index} -> flow_lmdb_path(ctx, key) end)
          |> Enum.reduce(%{}, fn {path, indexed_keys}, acc ->
            group_keys = Enum.map(indexed_keys, fn {key, _index} -> key end)
            values = flow_lmdb_get_many(ctx, path, group_keys, now_ms, mode)

            indexed_keys
            |> Enum.zip(values)
            |> Enum.reduce(acc, fn {{_key, index}, value}, value_acc ->
              Map.put(value_acc, index, value)
            end)
          end)

        Enum.map(0..(length(keys) - 1)//1, &Map.get(values_by_index, &1))
      end

      defp flow_get_lmdb(ctx, key, mode) do
        path = flow_lmdb_path(ctx, key)

        value =
          path
          |> Ferricstore.Flow.LMDB.get(key)
          |> flow_decode_lmdb_get_result(path, key, CommandTime.now_ms(), mode)

        value || flow_get_lmdb_cold_park(ctx, key, mode)
      end

      defp flow_lmdb_path(ctx, key) do
        idx = shard_for(ctx, key)
        ctx.data_dir |> Ferricstore.DataDir.shard_data_path(idx) |> Ferricstore.Flow.LMDB.path()
      end

      defp flow_state_expired_or_deleted?(ctx, key, now_ms) when is_binary(key) do
        idx = shard_for(ctx, key)

        case Map.get(ctx, :keydir_refs) do
          refs when is_tuple(refs) and idx >= 0 and idx < tuple_size(refs) ->
            case :ets.lookup(elem(refs, idx), key) do
              [{^key, nil, _expire_at_ms, :flow_state_deleted, :deleted, _offset, _value_size}] ->
                true

              [{^key, _value, expire_at_ms, _lfu, _fid, _offset, _value_size}]
              when is_integer(expire_at_ms) and expire_at_ms > 0 and expire_at_ms <= now_ms ->
                true

              _other ->
                false
            end

          _other ->
            false
        end
      rescue
        _ -> false
      end

      defp flow_state_expired_or_deleted?(_ctx, _key, _now_ms), do: false

      defp flow_lmdb_get_many(_ctx, _path, [], _now_ms, _mode), do: []

      defp flow_lmdb_get_many(ctx, path, keys, now_ms, mode) do
        case Ferricstore.Flow.LMDB.get_many(path, keys) do
          {:ok, results} ->
            keys
            |> Enum.zip(results)
            |> Enum.map(fn {key, result} ->
              flow_decode_lmdb_get_result(result, path, key, now_ms, mode) ||
                flow_get_lmdb_cold_park(ctx, key, mode)
            end)

          {:error, reason} ->
            flow_observe_lmdb_read_error(mode, reason)

            Enum.map(keys, fn key ->
              value =
                path
                |> Ferricstore.Flow.LMDB.get(key)
                |> flow_decode_lmdb_get_result(path, key, now_ms, mode)

              value || flow_get_lmdb_cold_park(ctx, key, mode)
            end)
        end
      end

      defp flow_get_lmdb_cold_park(ctx, key, mode) do
        path = flow_lmdb_path(ctx, key)
        park_key = Ferricstore.Flow.LMDB.cold_park_key_for_state_key(key)

        with {:ok, park_blob} <- Ferricstore.Flow.LMDB.get(path, park_key),
             {:ok, %{locator: %Locator{kind: :state} = locator} = park} <-
               Ferricstore.Flow.LMDB.decode_cold_park(park_blob),
             {:ok, value} <- flow_read_cold_park_state_value(ctx, key, locator, park),
             {:ok, record} <- flow_decode_cold_park_state(value),
             true <- flow_locator_matches_record?(locator, record) do
          value
        else
          {:error, reason} ->
            flow_lmdb_read_error_result(mode, reason)

          _ ->
            nil
        end
      end

      defp flow_read_state_locator_value(
             ctx,
             key,
             %Locator{file_id: fid, offset: offset, value_size: value_size}
           )
           when valid_cold_location(fid, offset, value_size) do
        idx = shard_for(ctx, key)

        case Ferricstore.Store.ColdRead.pread_keyed(
               cold_file_path(ctx, idx, fid),
               offset,
               key,
               @cold_batch_read_timeout_ms
             ) do
          {:ok, value} when is_binary(value) -> {:ok, value}
          {:error, reason} -> {:error, reason}
          _ -> :not_found
        end
      end

      defp flow_read_state_locator_value(
             ctx,
             key,
             %Locator{file_id: fid, offset: offset, value_size: value_size}
           )
           when valid_waraft_segment_location(fid, offset, value_size) do
        idx = shard_for(ctx, key)

        case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(ctx, idx, fid, key) do
          {:ok, value} when is_binary(value) -> {:ok, value}
          :not_found -> :not_found
          {:error, reason} -> {:error, reason}
        end
      end

      defp flow_read_state_locator_value(_ctx, _key, _locator), do: :not_found

      defp flow_read_cold_park_state_value(ctx, key, %Locator{} = locator, park)
           when is_map(park) do
        case flow_read_state_locator_value(ctx, key, locator) do
          {:ok, value} ->
            {:ok, value}

          _ ->
            case Map.get(park, :state_value) do
              value when is_binary(value) -> {:ok, value}
              _ -> :not_found
            end
        end
      end

      defp flow_decode_cold_park_state(value) when is_binary(value) do
        try do
          {:ok, Ferricstore.Flow.decode_record(value)}
        rescue
          _ -> {:error, :decode_error}
        end
      end

      defp flow_locator_matches_record?(%Locator{} = locator, record) do
        Map.get(record, :id) == locator.flow_id and Map.get(record, :version) == locator.version
      end

      defp flow_decode_lmdb_get_result({:ok, blob}, path, key, now_ms, mode)
           when is_binary(blob) do
        case Ferricstore.Flow.LMDB.decode_value(blob, now_ms) do
          {:ok, value} ->
            if flow_terminal_record_expired?(value, now_ms) do
              Ferricstore.Flow.LMDB.delete_state_artifacts(path, key)
              nil
            else
              value
            end

          :expired ->
            Ferricstore.Flow.LMDB.delete_state_artifacts(path, key)
            nil

          :error ->
            flow_lmdb_read_error_result(mode, :decode_error)
        end
      end

      defp flow_decode_lmdb_get_result(:not_found, _path, _key, _now_ms, _mode), do: nil

      defp flow_decode_lmdb_get_result({:error, reason}, _path, _key, _now_ms, mode),
        do: flow_lmdb_read_error_result(mode, reason)

      defp flow_decode_lmdb_get_result(_other, _path, _key, _now_ms, mode),
        do: flow_lmdb_read_error_result(mode, :unexpected_result)

      defp flow_terminal_record_expired?(value, now_ms)
           when is_binary(value) and is_integer(now_ms) do
        try do
          record = Ferricstore.Flow.decode_record(value)

          Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) and
            case Map.get(record, :terminal_retention_until_ms) do
              expire_at_ms when is_integer(expire_at_ms) and expire_at_ms <= now_ms -> true
              _other -> false
            end
        rescue
          _ -> false
        end
      end

      defp flow_terminal_record_expired?(_value, _now_ms), do: false

      defp flow_lmdb_read_error_result(mode, reason) do
        flow_observe_lmdb_read_error(mode, reason)
        nil
      end

      defp flow_observe_lmdb_read_error(mode, reason) do
        :telemetry.execute(
          [:ferricstore, :flow, :lmdb, :read_error],
          %{count: 1},
          %{mode: mode, reason: reason}
        )
      end

      @doc false
      def flow_create(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        cond do
          byte_size(key) > @max_key_size ->
            {:error, "ERR key too large (max #{@max_key_size} bytes)"}

          flow_create_admission_rejected?(ctx, key) ->
            flow_create_overloaded_error()

          true ->
            idx = shard_for(ctx, key)
            raft_write(ctx, idx, key, {:flow_create, key, attrs})
        end
      end

      @doc false
      def flow_named_value_put(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:flow_named_value_put, key, attrs})
        end
      end

      @doc false
      def flow_signal(ctx, %{id: id} = attrs) when is_binary(id) do
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:flow_signal, key, attrs})
        end
      end

      @doc false
      def flow_create_batch(_ctx, []), do: []

      def flow_create_batch(ctx, attrs_list) when is_list(attrs_list) do
        if ctx.name == :default do
          {valid, indexed_results} =
            attrs_list
            |> Enum.with_index()
            |> Enum.reduce({[], %{}}, fn
              {%{id: id} = attrs, idx}, {valid_acc, result_acc} when is_binary(id) ->
                key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))

                cond do
                  byte_size(key) > @max_key_size ->
                    {valid_acc,
                     Map.put(
                       result_acc,
                       idx,
                       {:error, "ERR key too large (max #{@max_key_size} bytes)"}
                     )}

                  flow_create_admission_rejected?(ctx, key) ->
                    {valid_acc, Map.put(result_acc, idx, flow_create_overloaded_error())}

                  true ->
                    {[{idx, key, {:flow_create, key, attrs}} | valid_acc], result_acc}
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
          Enum.map(attrs_list, &flow_create(ctx, &1))
        end
      end

      @doc false
      def flow_create_many(ctx, partition_key, attrs_list)
          when is_binary(partition_key) and is_list(attrs_list) do
        key = Ferricstore.Flow.Keys.state_key("__batch__", partition_key)

        cond do
          byte_size(key) > @max_key_size ->
            {:error, "ERR key too large (max #{@max_key_size} bytes)"}

          flow_create_many_admission_rejected?(ctx, attrs_list, partition_key) ->
            flow_create_overloaded_error(length(attrs_list))

          true ->
            idx = shard_for(ctx, key)

            raft_write(
              ctx,
              idx,
              key,
              {:flow_create_many, key, %{records: attrs_list}}
            )
        end
      end

      def flow_create_many(ctx, nil, attrs_list) when is_list(attrs_list) do
        if flow_create_many_admission_rejected?(ctx, attrs_list, nil) do
          flow_create_overloaded_error(length(attrs_list))
        else
          flow_many_by_shard(ctx, attrs_list, :flow_create_many, "__batch__")
        end
      end

      @doc false
      def flow_create_many_independent(_ctx, []), do: []

      def flow_create_many_independent(ctx, attrs_list) when is_list(attrs_list) do
        if flow_create_many_admission_rejected?(ctx, attrs_list, nil) do
          flow_create_overloaded_error(length(attrs_list))
        else
          flow_create_pipeline_batch(ctx, attrs_list)
        end
      end

      @doc false
      def flow_spawn_children(ctx, %{id: id, partition_key: partition_key} = attrs)
          when is_binary(id) and is_binary(partition_key) do
        key = Ferricstore.Flow.Keys.state_key(id, partition_key)

        if byte_size(key) > @max_key_size do
          {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        else
          child_keys =
            attrs
            |> Map.get(:children, [])
            |> Enum.map(fn %{id: child_id, partition_key: child_partition} ->
              Ferricstore.Flow.Keys.state_key(child_id, child_partition)
            end)

          keys = [key | child_keys]

          if flow_keys_cross_shard?(ctx, keys) do
            flow_cross_shard_tx(ctx, keys, {:flow_cross_spawn_children, attrs})
          else
            idx = shard_for(ctx, key)
            raft_write(ctx, idx, key, {:flow_spawn_children, key, attrs})
          end
        end
      end

      defp flow_keys_cross_shard?(ctx, [first | rest]) do
        first_idx = shard_for(ctx, first)
        Enum.any?(rest, &(shard_for(ctx, &1) != first_idx))
      end

      defp flow_keys_cross_shard?(_ctx, _keys), do: false

      defp flow_cross_shard_tx(ctx, keys, entry) do
        _route_keys = keys

        command = flow_cross_shard_tx_command(ctx, entry)
        anchor_idx = 0

        result =
          raft_write(ctx, anchor_idx, "f:{flow-cross-shard}:tx", command)

        case result do
          %{^anchor_idx => [reply]} -> reply
          {:error, _reason} = error -> error
          other -> other
        end
      end

      defp flow_cross_shard_tx_command(ctx, entry) do
        # Flow child closure can discover additional child shards while applying
        # parent terminal policies, so take a conservative all-shard transaction.
        shards = Enum.to_list(0..(ctx.shard_count - 1))
        anchor_idx = hd(shards)

        shard_batches =
          Enum.map(shards, fn
            ^anchor_idx -> {anchor_idx, [{0, entry}], nil}
            shard_idx -> {shard_idx, [], nil}
          end)

        {:cross_shard_tx, shard_batches}
      end

      defp flow_many_by_shard(ctx, attrs_list, command, _batch_id)
           when command in [
                  :flow_create_many,
                  :flow_complete_many,
                  :flow_cancel_many,
                  :flow_fail_many,
                  :flow_retry_many,
                  :flow_transition_many,
                  :flow_run_steps_many
                ] do
        {buckets, count} =
          Ferricstore.LatencyTrace.span("server_flow_many_bucket_us", fn ->
            Enum.reduce(attrs_list, {flow_fixed_shard_buckets(ctx.shard_count), 0}, fn
              %{id: id} = attrs, {buckets, idx} when is_binary(id) ->
                partition_key = Map.get(attrs, :partition_key)
                key = Ferricstore.Flow.Keys.state_key(id, partition_key)
                shard_idx = shard_for(ctx, key)

                {flow_put_shard_bucket(buckets, shard_idx, {idx, key, attrs}), idx + 1}
            end)
          end)

        groups =
          Ferricstore.LatencyTrace.span("server_flow_many_groups_us", fn ->
            flow_nonempty_shard_buckets(buckets, ctx.shard_count, fn shard_idx, entries ->
              group = Enum.reverse(entries)
              route_key = group |> hd() |> elem(1)
              attrs = Enum.map(group, fn {_idx, _key, attrs} -> attrs end)
              original_indices = Enum.map(group, fn {idx, _key, _attrs} -> idx end)
              command_attrs = flow_many_command_attrs(command, attrs)
              command_attrs = flow_stamp_shard(command_attrs, shard_idx)
              {shard_idx, route_key, original_indices, {command, route_key, command_attrs}}
            end)
          end)

        case Enum.find(groups, fn {_shard_idx, key, _indices, _cmd} ->
               byte_size(key) > @max_key_size
             end) do
          {_shard_idx, _key, indices, _cmd} ->
            error = {:error, "ERR key too large (max #{@max_key_size} bytes)"}
            {:ok, flow_many_error_results(count, indices, error)}

          nil ->
            keyed_commands =
              Ferricstore.LatencyTrace.span("server_flow_many_keyed_commands_us", fn ->
                Enum.map(groups, fn {_shard_idx, key, _indices, cmd} -> {key, cmd} end)
              end)

            group_results =
              Ferricstore.LatencyTrace.span("server_flow_many_quorum_us", fn ->
                batch_quorum_commands(ctx, keyed_commands)
              end)

            Ferricstore.LatencyTrace.span("server_flow_many_expand_us", fn ->
              expand_flow_many_results(count, groups, group_results)
            end)
        end
      end

      defp flow_many_command_attrs(:flow_transition_many, attrs_list),
        do: flow_transition_many_command_attrs(attrs_list)

      defp flow_many_command_attrs(_command, attrs_list), do: %{records: attrs_list}

      defp flow_stamp_shard(attrs, shard_idx) when is_map(attrs) and is_integer(shard_idx),
        do: Map.put(attrs, @flow_shard_marker, shard_idx)

      @flow_transition_many_shared_keys [
        :from_state,
        :to_state,
        :now_ms,
        :run_at_ms,
        :priority,
        :payload,
        :payload_ref,
        :values,
        :value_refs,
        :drop_values,
        :override_values
      ]

      defp flow_transition_many_command_attrs([_ | _] = attrs_list) do
        case flow_extract_shared_attrs(attrs_list, @flow_transition_many_shared_keys) do
          {%{} = shared, records} when map_size(shared) > 0 ->
            %{records: records, shared: shared}

          {_shared, records} ->
            %{records: records}
        end
      end

      defp flow_transition_many_command_attrs(attrs_list), do: %{records: attrs_list}

      defp flow_extract_shared_attrs(attrs_list, shared_keys) do
        Enum.reduce(shared_keys, {%{}, attrs_list}, fn key, {shared, records} ->
          case flow_shared_attr_value(records, key) do
            {:ok, value} ->
              {Map.put(shared, key, value), Enum.map(records, &Map.delete(&1, key))}

            :error ->
              {shared, records}
          end
        end)
      end

      defp flow_shared_attr_value([first | rest], key) do
        with {:ok, value} <- Map.fetch(first, key),
             true <- Enum.all?(rest, &(Map.has_key?(&1, key) and Map.fetch!(&1, key) == value)) do
          {:ok, value}
        else
          _ -> :error
        end
      end

      defp flow_many_error_results(count, error_indices, error) do
        error_set = MapSet.new(error_indices)

        0..(count - 1)
        |> Enum.map(fn idx ->
          if MapSet.member?(error_set, idx), do: error, else: ErrorReasons.write_timeout_unknown()
        end)
      end

      defp expand_flow_many_results(count, groups, group_results) do
        if Enum.all?(group_results, &(&1 == :ok)) do
          :ok
        else
          expanded =
            group_results
            |> Enum.zip(groups)
            |> Enum.reduce(%{}, fn
              {{:ok, records}, {_shard_idx, _key, indices, _cmd}}, acc when is_list(records) ->
                indices
                |> Enum.zip(records)
                |> Enum.reduce(acc, fn {idx, record}, next -> Map.put(next, idx, record) end)

              {{:error, _reason} = error, {_shard_idx, _key, indices, _cmd}}, acc ->
                Enum.reduce(indices, acc, fn idx, next -> Map.put(next, idx, error) end)

              {other, {_shard_idx, _key, indices, _cmd}}, acc ->
                Enum.reduce(indices, acc, fn idx, next -> Map.put(next, idx, other) end)
            end)

          results =
            0..(count - 1)
            |> Enum.map(fn idx ->
              Map.get(expanded, idx, ErrorReasons.write_timeout_unknown())
            end)

          {:ok, results}
        end
      end
    end
  end
end
