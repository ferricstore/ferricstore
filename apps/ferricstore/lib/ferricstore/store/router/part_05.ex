defmodule Ferricstore.Store.Router.Part05 do
  @moduledoc false

  # Extracted from Router: sampled_read_bookkeeping_batch .. apply_shard_results
  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.CommandTime
      alias Ferricstore.ErrorReasons
      alias Ferricstore.HLC
      alias Ferricstore.HyperLogLog, as: HLL
      alias Ferricstore.Flow.Locator
      alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
      alias Ferricstore.Flow.PolicyCommand
      alias Ferricstore.Raft.ReplyAwaiter
      alias Ferricstore.Stats
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore
      alias Ferricstore.Store.BlobValue
      alias Ferricstore.Store.CompoundCommand
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.DiskPressure
      alias Ferricstore.Store.Keydir
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.ListOps
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.SlotMap
      alias Ferricstore.Store.TypeRegistry

      defp hot_read_bookkeeping_start(ctx) do
        if Stats.cache_tracking_enabled?() do
          case Stats.start_keyspace_hit_batch(ctx, 0) do
            {:exact, 0} ->
              {:exact, 0, []}

            {:sampled_no_touch, rate, previous, 0} ->
              {:sampled, rate, previous, 0, rate - previous, []}

            {:sampled_touch, rate, previous, 0, next_sample_offset} ->
              {:sampled, rate, previous, 0, next_sample_offset, []}
          end
        else
          :disabled
        end
      end

      defp hot_read_bookkeeping_add(:disabled, _keydir, _key, _lfu), do: :disabled

      defp hot_read_bookkeeping_add({:exact, count, hits} = state, keydir, key, lfu) do
        if Stats.cache_tracking_key?(key) do
          {:exact, count + 1, [{keydir, key, lfu} | hits]}
        else
          state
        end
      end

      defp hot_read_bookkeeping_add(
             {:sampled, rate, previous, count, next_sample_offset, hits} = state,
             keydir,
             key,
             lfu
           ) do
        if Stats.cache_tracking_key?(key) do
          count = count + 1

          hits =
            if count >= next_sample_offset and rem(count - next_sample_offset, rate) == 0 do
              [{keydir, key, lfu} | hits]
            else
              hits
            end

          {:sampled, rate, previous, count, next_sample_offset, hits}
        else
          state
        end
      end

      defp hot_read_bookkeeping_finish(_ctx, :disabled), do: :ok

      defp hot_read_bookkeeping_finish(ctx, {:exact, count, hits}) do
        :ok = Stats.finish_keyspace_hit_batch(ctx, {:exact, count})
        touch_hot_read_entries(ctx, hits)
      end

      defp hot_read_bookkeeping_finish(ctx, {:sampled, rate, previous, count, offset, hits}) do
        :ok =
          Stats.finish_keyspace_hit_batch(ctx, {:sampled_touch, rate, previous, count, offset})

        touch_hot_read_entries(ctx, hits)
      end

      defp touch_hot_read_entries(ctx, hits) do
        Enum.each(hits, fn {keydir, key, lfu} ->
          LFU.touch(ctx, keydir, key, lfu)
          Stats.record_hot_read(ctx, key)
        end)
      end

      defp sampled_read_bookkeeping_batch(_ctx, [], _max_hits), do: :ok

      defp sampled_read_bookkeeping_batch(ctx, hot_hits, max_hits)
           when is_list(hot_hits) and is_integer(max_hits) and max_hits >= 0 do
        if Stats.cache_tracking_enabled?() do
          hot_hits =
            Enum.filter(hot_hits, fn {_keydir, key, _lfu} -> Stats.cache_tracking_key?(key) end)

          max_hits = length(hot_hits)
          sample_state = Stats.start_keyspace_hit_batch(ctx, max_hits)
          touched = sampled_hit_entries(sample_state, hot_hits)

          :ok =
            Stats.finish_keyspace_hit_batch(ctx, finish_hit_batch_state(sample_state, max_hits))

          Enum.each(touched, fn {keydir, key, lfu} ->
            LFU.touch(ctx, keydir, key, lfu)
            Stats.record_hot_read(ctx, key)
          end)
        else
          :ok
        end
      end

      defp sampled_hit_entries({:exact, _count}, hot_hits), do: hot_hits

      defp sampled_hit_entries({:sampled_no_touch, _rate, _previous, _count}, _hot_hits), do: []

      defp sampled_hit_entries(
             {:sampled_touch, rate, _previous, _count, next_sample_offset},
             hot_hits
           ) do
        hot_hits
        |> Enum.with_index(1)
        |> Enum.flat_map(fn
          {hit, offset}
          when offset >= next_sample_offset and rem(offset - next_sample_offset, rate) == 0 ->
            [hit]

          _other ->
            []
        end)
      end

      defp finish_hit_batch_state({:exact, _count}, count), do: {:exact, count}

      defp finish_hit_batch_state({:sampled_no_touch, rate, previous, _count}, count),
        do: {:sampled_no_touch, rate, previous, count}

      defp finish_hit_batch_state({:sampled_touch, rate, previous, _count, offset}, count),
        do: {:sampled_touch, rate, previous, count, offset}

      defp record_keyspace_miss(ctx, key) do
        Stats.sample_keyspace_misses_for_key(ctx, key)
      end

      # After a cold read, promote the value back to ETS (hot) if it fits
      # under the hot cache max value size threshold. ETS is :public with
      # write_concurrency so this is safe from any process.
      @doc false
      def warm_ets_after_cold_read(ctx, keydir, key, value, file_id, offset) do
        warm_ets_after_cold_read(
          ctx,
          keydir_index(ctx, keydir),
          keydir,
          key,
          value,
          file_id,
          offset
        )
      end

      @doc false
      def warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset) do
        # Skip promotion when under memory pressure — prevents evict/re-promote
        # thrashing where MemoryGuard evicts values and cold reads immediately
        # re-cache them. skip_promotion? is set at :pressure level (85%+).
        skip_promotion = :atomics.get(ctx.pressure_flags, 3) == 1

        if byte_size(value) <= ctx.hot_cache_max_value_size and not skip_promotion do
          lfu = LFU.initial()

          try do
            case :ets.lookup(keydir, key) do
              [
                {^key, nil, expire_at_ms, old_lfu, ^file_id, ^offset, value_size} = observed
              ] ->
                replacement =
                  {key, value, expire_at_ms, lfu, file_id, offset, value_size}

                if Keydir.replace_exact(keydir, observed, replacement) do
                  track_keydir_binary_warm(ctx, idx, value)
                end

              _other ->
                :ok
            end

            :ok
          rescue
            ArgumentError -> :ok
          end
        end
      end

      defp keydir_index(%{keydir_refs: refs, shard_count: count}, keydir)
           when is_tuple(refs) and is_integer(count) and count > 0 do
        Enum.find(0..(count - 1), fn idx -> elem(refs, idx) == keydir end)
      end

      defp keydir_index(_ctx, _keydir), do: nil

      defp track_keydir_binary_warm(%{keydir_binary_bytes: ref}, idx, value)
           when is_integer(idx) do
        bytes = offheap_size(value)

        if bytes > 0 and idx >= 0 and idx < :atomics.info(ref).size do
          :atomics.add(ref, idx + 1, bytes)
        end
      end

      defp track_keydir_binary_warm(_ctx, _idx, _value), do: :ok

      @max_key_size 65_535
      @max_value_size 512 * 1024 * 1024

      @spec max_key_size() :: pos_integer()
      @doc "Returns the maximum allowed key size in bytes."
      def max_key_size, do: @max_key_size

      @spec max_value_size() :: pos_integer()
      @doc "Returns the maximum allowed value size in bytes."
      def max_value_size, do: @max_value_size

      @crossslot_error {:error, "CROSSSLOT Keys in request don't hash to the same slot"}

      @doc "Atomically writes one hash slot of string key-value pairs."
      @spec atomic_mset(FerricStore.Instance.t(), [{binary(), binary()}]) ::
              :ok | {:error, term()}
      def atomic_mset(_ctx, []), do: :ok

      def atomic_mset(ctx, kv_pairs) when is_list(kv_pairs) do
        with {:ok, shard_index, entries} <- prepare_atomic_string_batch(ctx, kv_pairs),
             :ok <- admit_atomic_string_batch(ctx, shard_index, entries) do
          submit_atomic_string_batch(ctx, shard_index, {:mset, entries})
        end
      end

      @doc "Atomically writes one hash slot only when every target key is absent."
      @spec atomic_msetnx(FerricStore.Instance.t(), [{binary(), binary()}]) ::
              0 | 1 | {:error, term()}
      def atomic_msetnx(_ctx, []), do: 1

      def atomic_msetnx(ctx, kv_pairs) when is_list(kv_pairs) do
        with {:ok, shard_index, entries} <- prepare_atomic_string_batch(ctx, kv_pairs),
             :ok <- admit_atomic_string_batch(ctx, shard_index, entries) do
          submit_atomic_string_batch(ctx, shard_index, {:msetnx, entries})
        end
      end

      @doc false
      @spec admit_string_batch(FerricStore.Instance.t(), [{binary(), binary()}]) ::
              :ok | {:error, term()}
      def admit_string_batch(_ctx, []), do: :ok

      def admit_string_batch(ctx, kv_pairs) when is_list(kv_pairs) do
        max_value_size = configured_max_value_size(ctx)

        Enum.reduce_while(kv_pairs, :ok, fn entry, :ok ->
          case admit_batch_put_entry(ctx, entry, max_value_size, true) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp prepare_atomic_string_batch(ctx, [{key, value} | rest]) do
        with :ok <- validate_atomic_string_entry(ctx, key, value) do
          slot = SlotMap.slot_for_key(key)
          shard_index = elem(ctx.slot_map, slot)
          prepare_atomic_string_batch(ctx, rest, slot, shard_index, [{key, value, 0}])
        end
      end

      defp prepare_atomic_string_batch(_ctx, _invalid),
        do: {:error, "ERR invalid key-value batch"}

      defp prepare_atomic_string_batch(_ctx, [], _slot, shard_index, entries),
        do: {:ok, shard_index, Enum.reverse(entries)}

      defp prepare_atomic_string_batch(
             ctx,
             [{key, value} | rest],
             slot,
             shard_index,
             entries
           ) do
        with :ok <- validate_atomic_string_entry(ctx, key, value) do
          if SlotMap.slot_for_key(key) == slot do
            prepare_atomic_string_batch(ctx, rest, slot, shard_index, [
              {key, value, 0} | entries
            ])
          else
            @crossslot_error
          end
        end
      end

      defp prepare_atomic_string_batch(_ctx, _invalid, _slot, _shard_index, _entries),
        do: {:error, "ERR invalid key-value batch"}

      defp validate_atomic_string_entry(ctx, key, value) do
        with :ok <- validate_string_write(ctx, key, value) do
          if key == "", do: {:error, "ERR key too large or empty"}, else: :ok
        end
      end

      defp admit_atomic_string_batch(ctx, shard_index, entries) do
        cond do
          DiskPressure.under_pressure?(ctx, shard_index) ->
            {:error, "ERR disk pressure on shard #{shard_index}, rejecting write"}

          true ->
            Enum.reduce_while(entries, :ok, fn {key, _value, _expire_at_ms}, :ok ->
              case check_keydir_full(ctx, key) do
                :ok -> {:cont, :ok}
                {:error, _reason} = error -> {:halt, error}
              end
            end)
        end
      end

      defp submit_atomic_string_batch(ctx, shard_index, command) do
        {_, [{key, _value, _expire_at_ms} | _]} = {elem(command, 0), elem(command, 1)}

        if durable_raft_ctx?(ctx) do
          raft_write(ctx, shard_index, key, command)
        else
          GenServer.call(elem(ctx.shard_names, shard_index), {:standalone_commit, command})
        end
      end

      defp configured_max_value_size(ctx) do
        case Map.get(ctx, :max_value_size, 1_048_576) do
          value when is_integer(value) and value > 0 -> min(value, @max_value_size)
          _invalid -> min(1_048_576, @max_value_size)
        end
      end

      defp validate_string_write(ctx, key, value) do
        validate_string_write(ctx, key, value, configured_max_value_size(ctx))
      end

      defp validate_string_write(_ctx, key, value, max_value_size)
           when is_binary(key) and is_binary(value) do
        cond do
          byte_size(key) > @max_key_size ->
            {:error, "ERR key too large (max #{@max_key_size} bytes)"}

          byte_size(value) > max_value_size ->
            {:error,
             "ERR value too large (#{byte_size(value)} bytes, max #{max_value_size} bytes)"}

          true ->
            :ok
        end
      end

      defp validate_string_write(_ctx, _key, _value, _max_value_size),
        do: {:error, "ERR invalid key-value batch"}

      defp admit_string_write(ctx, key, value) do
        with :ok <- validate_string_write(ctx, key, value) do
          shard_index = shard_for(ctx, key)

          cond do
            DiskPressure.under_pressure?(ctx, shard_index) ->
              {:error, "ERR disk pressure on shard #{shard_index}, rejecting write"}

            true ->
              case check_keydir_full(ctx, key) do
                :ok -> {:ok, shard_index}
                {:error, _reason} = error -> error
              end
          end
        end
      end

      @doc """
      Batch PUT API with `:ok | {:error, _}` result shape.

      The default application instance submits through quorum. Embedded/custom
      instances write locally because the Raft batchers are owned by the default
      application instance.
      """
      @spec batch_put(FerricStore.Instance.t(), [{binary(), binary()}]) ::
              :ok | {:error, binary()}
      def batch_put(ctx, kv_pairs) do
        case batch_quorum_put(ctx, kv_pairs) do
          results when is_list(results) ->
            Enum.find(results, &match?({:error, _}, &1)) || :ok

          other ->
            other
        end
      end

      @doc false
      def __install_batch_entries_for_test__(ctx, idx, entries, shard_locations) do
        keydir = elem(ctx.keydir_refs, idx)
        install_batch_entries(ctx, idx, keydir, entries, shard_locations, LFU.initial())
      end

      defp install_batch_entries(ctx, idx, keydir, entries, shard_locations, lfu_val) do
        Enum.each(entries, fn {key, value, value_for_ets} ->
          clear_compound_data_structure_for_string_put(ctx, idx, keydir, key)

          case Map.get(shard_locations, key) do
            {file_id, offset, value_size} ->
              track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)
              :ets.insert(keydir, {key, value_for_ets, 0, lfu_val, file_id, offset, value_size})

            nil ->
              install_batch_pending_entry(ctx, idx, keydir, key, value, value_for_ets, lfu_val)
          end
        end)
      end

      defp install_batch_pending_entry(ctx, idx, keydir, key, value, value_for_ets, lfu_val) do
        case :ets.lookup(keydir, key) do
          [{^key, ^value_for_ets, 0, _lfu, fid, off, value_size}]
          when fid != :pending and valid_cold_location(fid, off, value_size) ->
            :ok

          _ ->
            track_keydir_binary_insert(ctx, idx, keydir, key, value_for_ets)
            :ets.insert(keydir, {key, value_for_ets, 0, lfu_val, :pending, 0, byte_size(value)})
        end
      end

      @doc """
      Batch quorum PUT for pipelined SET commands.

      Groups commands by shard, submits each group as a single batch to its
      Batcher with ONE reply ref per shard, then waits for all shard replies.
      Returns a list of results in input order.

      Uses one ref per shard (not per command) so the connection process's
      selective receive scans at most shard_count refs instead of N*pipeline
      refs — critical for high concurrency where TCP messages flood the
      mailbox.
      """
      @spec batch_quorum_put(FerricStore.Instance.t(), [{binary(), binary()}]) :: [
              :ok | {:error, binary() | {:timeout, :unknown_outcome}}
            ]
      def batch_quorum_put(ctx, kv_pairs) do
        batch_quorum_put(ctx, kv_pairs, nil)
      end

      @doc false
      @spec batch_quorum_put_status(FerricStore.Instance.t(), [{binary(), binary()}]) ::
              :ok | {:error, term()}
      def batch_quorum_put_status(ctx, kv_pairs) do
        batch_quorum_put_status(ctx, kv_pairs, nil)
      end

      @doc false
      def __batch_result_status_for_test__(results), do: batch_results_status(results)

      @doc false
      def __normalize_batch_write_result_for_test__(result, expected_count),
        do: normalize_batch_write_result(result, expected_count)

      defp batch_quorum_commands(ctx, keyed_commands) do
        if ctx.name == :default do
          batch_quorum_commands(ctx, keyed_commands, nil)
        else
          Enum.map(keyed_commands, fn {key, command} ->
            raft_write(ctx, shard_for(ctx, key), key, command)
          end)
        end
      end

      @doc false
      def pipeline_write_batch(_ctx, []), do: []

      def pipeline_write_batch(ctx, keyed_commands) when is_list(keyed_commands) do
        if ctx.name == :default do
          # Homogeneous hot pipeline batches go straight to final Ra command shapes.
          # That avoids allocating per-key command tuples just to compact them later.
          # The matching state-machine path must stay equally compact: for pure
          # writes, stage disk records first and publish ETS only after append
          # succeeds.
          case direct_batch_command_shape(keyed_commands) do
            {:put, entries} -> batch_quorum_put(ctx, entries, nil)
            {:delete, keys} -> do_batch_quorum_delete_keys(ctx, keys, nil)
            {:zadd_many_single, entries} -> batch_quorum_zadd_many_single(ctx, entries)
            :generic -> batch_quorum_commands(ctx, keyed_commands)
          end
        else
          Enum.map(keyed_commands, fn {key, command} ->
            idx = shard_for(ctx, key)
            raft_write(ctx, idx, key, command)
          end)
        end
      end

      @doc false
      def flow_command_batch(ctx, keyed_commands), do: pipeline_write_batch(ctx, keyed_commands)

      defp direct_batch_command_shape([]), do: :generic

      defp direct_batch_command_shape(keyed_commands) do
        direct_batch_command_shape(keyed_commands, :unknown, [])
      end

      defp direct_batch_command_shape([], :put, acc), do: {:put, Enum.reverse(acc)}
      defp direct_batch_command_shape([], :delete, acc), do: {:delete, Enum.reverse(acc)}

      defp direct_batch_command_shape([], :zadd_many_single, acc),
        do: {:zadd_many_single, Enum.reverse(acc)}

      defp direct_batch_command_shape([], _mode, _acc), do: :generic

      defp direct_batch_command_shape(
             [{_route_key, {:put, key, value, expire_at_ms}} | rest],
             mode,
             acc
           )
           when mode in [:unknown, :put] and is_binary(key) and is_binary(value) and
                  is_integer(expire_at_ms) do
        direct_batch_command_shape(rest, :put, [{key, value, expire_at_ms} | acc])
      end

      defp direct_batch_command_shape([{_route_key, {:delete, key}} | rest], mode, acc)
           when mode in [:unknown, :delete] and is_binary(key) do
        direct_batch_command_shape(rest, :delete, [key | acc])
      end

      defp direct_batch_command_shape(
             [{_route_key, {:zadd_single, key, score, member}} | rest],
             mode,
             acc
           )
           when mode in [:unknown, :zadd_many_single] and is_binary(key) and is_number(score) and
                  is_binary(member) do
        direct_batch_command_shape(rest, :zadd_many_single, [{key, score * 1.0, member} | acc])
      end

      defp direct_batch_command_shape(_commands, _mode, _acc), do: :generic

      defp batch_quorum_commands(_ctx, [], _origin_node), do: []

      defp batch_quorum_commands(ctx, keyed_commands, origin_node) do
        with {:ok, keyed_commands} <- maybe_stamp_flow_commands(ctx, keyed_commands, origin_node) do
          do_batch_quorum_commands_dispatch(ctx, keyed_commands, origin_node)
        else
          {:error, _reason} = error -> List.duplicate(error, length(keyed_commands))
        end
      end

      defp maybe_stamp_flow_commands(_ctx, keyed_commands, origin_node)
           when not is_nil(origin_node),
           do: {:ok, keyed_commands}

      defp maybe_stamp_flow_commands(ctx, keyed_commands, nil) do
        with :ok <- validate_flow_owned_batch_locality(ctx, keyed_commands) do
          PolicyCommand.stamp_many(ctx, keyed_commands)
        end
      end

      defp validate_flow_owned_batch_locality(ctx, keyed_commands) do
        Enum.reduce_while(keyed_commands, :ok, fn {key, command}, :ok ->
          idx = shard_for(ctx, key)

          case validate_flow_owned_write_locality(ctx, idx, key, command) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end

      defp do_batch_quorum_commands_dispatch(ctx, keyed_commands, origin_node) do
        do_batch_quorum_commands(ctx, keyed_commands, origin_node)
      end

      defp batch_quorum_zadd_many_single(_ctx, []), do: []

      defp batch_quorum_zadd_many_single(ctx, entries) do
        if selected_waraft_ctx?(ctx) do
          {by_shard, count} =
            Enum.reduce(entries, {%{}, 0}, fn {key, _score, _member} = entry, {shards, i} ->
              idx = shard_for(ctx, key)
              {shard_entries, indices} = Map.get(shards, idx, {[], []})

              {Map.put(shards, idx, {[entry | shard_entries], [i | indices]}), i + 1}
            end)

          initial_results = new_waraft_result_tuple(count)

          shard_batches =
            Enum.map(by_shard, fn {shard_idx, {shard_entries, indices}} ->
              {shard_idx, Enum.reverse(indices), Enum.reverse(shard_entries)}
            end)

          shard_results =
            case shard_batches do
              [{shard_idx, indices, shard_entries}] ->
                [
                  {shard_idx, indices,
                   Ferricstore.Raft.Backend.write(shard_idx, {:zadd_many_single, shard_entries})}
                ]

              batches ->
                commands =
                  Enum.map(batches, fn {shard_idx, _indices, shard_entries} ->
                    {shard_idx, {:zadd_many_single, shard_entries}}
                  end)

                batch_results = Ferricstore.Raft.Backend.write_many(commands)

                batches
                |> zip_batch_groups_with_results(batch_results)
                |> Enum.map(fn {{shard_idx, indices, _shard_entries}, result} ->
                  {shard_idx, indices, result}
                end)
            end

          shard_results
          |> Enum.reduce(initial_results, fn {shard_idx, indices, result}, acc ->
            merge_waraft_batch_results(ctx, shard_idx, indices, result, acc)
          end)
          |> Tuple.to_list()
        else
          keyed_commands =
            Enum.map(entries, fn {key, score, member} ->
              {key, {:zadd_single, key, score, member}}
            end)

          batch_quorum_commands(ctx, keyed_commands)
        end
      end

      defp do_batch_quorum_commands(ctx, keyed_commands, origin_node) do
        if selected_waraft_ctx?(ctx) do
          waraft_batch_commands(ctx, keyed_commands)
        else
          do_ra_batch_quorum_commands(ctx, keyed_commands, origin_node)
        end
      end

      defp do_ra_batch_quorum_commands(ctx, keyed_commands, origin_node) do
        wv_size = :counters.info(ctx.write_version).size

        {by_shard, count, by_shard_commands} =
          keyed_commands
          |> Enum.reduce({%{}, 0, %{}}, fn {key, command}, {shards, i, commands_map} ->
            idx = shard_for(ctx, key)
            {cmds, indices} = Map.get(shards, idx, {[], []})

            {
              Map.put(shards, idx, {[command | cmds], [i | indices]}),
              i + 1,
              Map.update(commands_map, idx, [{key, command}], fn acc -> [{key, command} | acc] end)
            }
          end)

        shard_refs =
          Enum.map(by_shard, fn {shard_idx, {cmds, indices}} ->
            {from, token} = ReplyAwaiter.new()
            cmds = Enum.reverse(cmds)

            if origin_node == nil do
              Ferricstore.Raft.Batcher.write_batch(shard_idx, cmds, from)
            else
              Ferricstore.Raft.Batcher.write_batch_forwarded(shard_idx, cmds, from, origin_node)
            end

            {token, shard_idx, Enum.reverse(indices)}
          end)

        results =
          collect_shard_replies(
            shard_refs,
            wv_size,
            ctx,
            %{},
            System.monotonic_time(:millisecond)
          )

        results =
          Enum.reduce(by_shard_commands, results, fn {shard_idx, commands}, acc ->
            {_cmds, indices} = Map.fetch!(by_shard, shard_idx)
            first_index = List.last(indices)

            case Map.get(acc, first_index) do
              {:error, {:not_leader, {_shard_name, leader_node}}} when is_atom(leader_node) ->
                merge_forwarded_commands(acc, by_shard, shard_idx, commands, leader_node, ctx)

              {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
                merge_forwarded_commands(acc, by_shard, shard_idx, commands, leader_node, ctx)

              _ ->
                acc
            end
          end)

        0..(count - 1)
        |> Enum.map(fn i -> Map.get(results, i, ErrorReasons.write_timeout_unknown()) end)
      end

      defp batch_quorum_put(_ctx, [], _origin_node), do: []

      defp batch_quorum_put(ctx, kv_pairs, origin_node) do
        case batch_put_admission(ctx, kv_pairs) do
          :ok ->
            execute_admitted_batch_put(ctx, kv_pairs, origin_node)

          {:partitioned, admitted, admitted_indices, rejected, count} ->
            admitted_results = execute_admitted_batch_put(ctx, admitted, origin_node)

            merge_batch_admission_results(
              admitted_results,
              admitted_indices,
              rejected,
              count
            )
        end
      end

      defp execute_admitted_batch_put(ctx, kv_pairs, origin_node) do
        if durable_raft_ctx?(ctx) do
          do_batch_quorum_put_entries(ctx, kv_pairs, origin_node)
        else
          local_batch_put_entries(ctx, kv_pairs)
        end
      end

      defp batch_put_admission(ctx, kv_pairs) do
        max_value_size = configured_max_value_size(ctx)
        pressure? = batch_write_pressure?(ctx)

        case batch_put_entries_valid?(ctx, kv_pairs, max_value_size, pressure?) do
          true -> :ok
          false -> partition_batch_put_entries(ctx, kv_pairs, max_value_size)
        end
      end

      defp batch_put_entries_valid?(ctx, entries, max_value_size, pressure?) do
        Enum.reduce_while(entries, true, fn entry, true ->
          case admit_batch_put_entry(ctx, entry, max_value_size, pressure?) do
            :ok -> {:cont, true}
            {:error, _reason} -> {:halt, false}
          end
        end)
      end

      defp partition_batch_put_entries(ctx, entries, max_value_size) do
        {admitted, admitted_indices, rejected, count} =
          Enum.reduce(entries, {[], [], %{}, 0}, fn entry, {admitted, indices, rejected, index} ->
            case admit_batch_put_entry(ctx, entry, max_value_size, true) do
              :ok ->
                {[entry | admitted], [index | indices], rejected, index + 1}

              {:error, _reason} = error ->
                {admitted, indices, Map.put(rejected, index, error), index + 1}
            end
          end)

        {:partitioned, Enum.reverse(admitted), Enum.reverse(admitted_indices), rejected, count}
      end

      defp admit_batch_put_entry(ctx, entry, max_value_size, pressure?) do
        with {:ok, key, value, _expire_at_ms} <-
               validate_batch_put_entry(ctx, entry, max_value_size) do
          if pressure? do
            shard_index = shard_for(ctx, key)

            cond do
              DiskPressure.under_pressure?(ctx, shard_index) ->
                {:error, "ERR disk pressure on shard #{shard_index}, rejecting write"}

              true ->
                check_keydir_full(ctx, key)
            end
          else
            :ok
          end
        end
      end

      defp validate_batch_put_entry(ctx, {key, value}, max_value_size) do
        with :ok <- validate_string_write(ctx, key, value, max_value_size) do
          {:ok, key, value, 0}
        end
      end

      defp validate_batch_put_entry(ctx, {key, value, expire_at_ms}, max_value_size)
           when is_integer(expire_at_ms) and expire_at_ms >= 0 do
        with :ok <- validate_string_write(ctx, key, value, max_value_size) do
          {:ok, key, value, expire_at_ms}
        end
      end

      defp validate_batch_put_entry(_ctx, _entry, _max_value_size),
        do: {:error, "ERR invalid key-value batch"}

      defp batch_write_pressure?(ctx) do
        :atomics.get(ctx.pressure_flags, 1) == 1 or
          :atomics.get(ctx.pressure_flags, 2) == 1 or
          Enum.any?(0..(ctx.shard_count - 1), &DiskPressure.under_pressure?(ctx, &1))
      end

      defp merge_batch_admission_results(admitted_results, indices, rejected, count) do
        results =
          Enum.reduce(rejected, :erlang.make_tuple(count, nil), fn {index, error}, results ->
            put_elem(results, index, error)
          end)

        normalized_results =
          case admitted_results do
            results when is_list(results) and length(results) == length(indices) ->
              results

            {:error, _reason} = error ->
              List.duplicate(error, length(indices))

            invalid ->
              List.duplicate({:error, {:invalid_batch_result, invalid}}, length(indices))
          end

        indices
        |> Enum.zip(normalized_results)
        |> Enum.reduce(results, fn {index, result}, results ->
          put_elem(results, index, result)
        end)
        |> Tuple.to_list()
      end

      @doc false
      @spec batch_quorum_delete(FerricStore.Instance.t(), [binary()]) :: [
              :ok | {:error, binary() | {:timeout, :unknown_outcome}}
            ]
      def batch_quorum_delete(ctx, keys) do
        batch_quorum_delete(ctx, keys, nil)
      end

      defp batch_quorum_delete(_ctx, [], _origin_node), do: []

      defp batch_quorum_delete(ctx, keys, origin_node) do
        cond do
          not durable_raft_ctx?(ctx) ->
            local_batch_delete_keys(ctx, keys)

          true ->
            do_batch_quorum_delete_keys(ctx, keys, origin_node)
        end
      end

      defp local_batch_put_entries(ctx, entries) do
        Enum.map(entries, fn entry ->
          {key, value, expire_at_ms} = normalize_put_batch_entry(entry)
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:put, key, value, expire_at_ms})
        end)
      end

      defp local_batch_delete_keys(ctx, keys) do
        Enum.map(keys, fn key ->
          idx = shard_for(ctx, key)
          raft_write(ctx, idx, key, {:delete, key})
        end)
      end

      defp waraft_batch_commands(_ctx, []), do: []

      defp waraft_batch_commands(ctx, keyed_commands) do
        {buckets, count} =
          Enum.reduce(keyed_commands, {new_waraft_batch_buckets(ctx.shard_count), 0}, fn {key,
                                                                                          command},
                                                                                         {buckets,
                                                                                          i} ->
            idx = shard_for(ctx, key)
            {put_waraft_batch_bucket(buckets, idx, command, i), i + 1}
          end)

        collect_waraft_shard_batches(
          ctx,
          waraft_batch_groups(buckets, ctx.shard_count),
          count,
          &Ferricstore.Raft.Backend.write_batch/2,
          &{:batch, &1}
        )
      end

      defp waraft_batch_put_entries(_ctx, []), do: []

      defp waraft_batch_put_entries(ctx, entries) do
        {buckets, count} =
          Enum.reduce(entries, {new_waraft_batch_buckets(ctx.shard_count), 0}, fn entry,
                                                                                  {buckets, i} ->
            {key, value, expire_at_ms} = normalize_put_batch_entry(entry)
            idx = shard_for(ctx, key)
            {put_waraft_batch_bucket(buckets, idx, {key, value, expire_at_ms}, i), i + 1}
          end)

        collect_waraft_hot_shard_batches(
          ctx,
          waraft_batch_groups(buckets, ctx.shard_count),
          count,
          &Ferricstore.Raft.WARaftBackend.write_put_batch_async/3,
          &Ferricstore.Raft.Backend.write_put_batch/2
        )
      end

      defp waraft_batch_put_entries_status(_ctx, []), do: :ok

      defp waraft_batch_put_entries_status(ctx, entries) do
        buckets =
          Enum.reduce(entries, new_waraft_item_buckets(ctx.shard_count), fn entry, buckets ->
            {key, value, expire_at_ms} = normalize_put_batch_entry(entry)
            idx = shard_for(ctx, key)
            put_waraft_item_bucket(buckets, idx, {key, value, expire_at_ms})
          end)

        collect_waraft_hot_shard_batch_status(
          ctx,
          waraft_item_groups(buckets, ctx.shard_count)
        )
      end

      defp waraft_batch_delete_keys(_ctx, []), do: []

      defp waraft_batch_delete_keys(ctx, keys) do
        {buckets, count} =
          Enum.reduce(keys, {new_waraft_batch_buckets(ctx.shard_count), 0}, fn key,
                                                                               {buckets, i} ->
            idx = shard_for(ctx, key)
            {put_waraft_batch_bucket(buckets, idx, key, i), i + 1}
          end)

        collect_waraft_hot_shard_batches(
          ctx,
          waraft_batch_groups(buckets, ctx.shard_count),
          count,
          &Ferricstore.Raft.WARaftBackend.write_delete_batch_async/3,
          &Ferricstore.Raft.Backend.write_delete_batch/2
        )
      end

      defp new_waraft_batch_buckets(shard_count) when is_integer(shard_count) and shard_count > 0,
        do: :erlang.make_tuple(shard_count, {[], []})

      defp put_waraft_batch_bucket(buckets, shard_idx, item, index) do
        {items, indices} = elem(buckets, shard_idx)
        put_elem(buckets, shard_idx, {[item | items], [index | indices]})
      end

      defp new_waraft_item_buckets(shard_count) when is_integer(shard_count) and shard_count > 0,
        do: :erlang.make_tuple(shard_count, {[], 0})

      defp put_waraft_item_bucket(buckets, shard_idx, item) do
        {items, count} = elem(buckets, shard_idx)
        put_elem(buckets, shard_idx, {[item | items], count + 1})
      end

      defp waraft_batch_groups(buckets, shard_count) do
        0..(shard_count - 1)
        |> Enum.reduce([], fn shard_idx, acc ->
          case elem(buckets, shard_idx) do
            {[], []} ->
              acc

            {items, indices} ->
              [{shard_idx, Enum.reverse(items), Enum.reverse(indices)} | acc]
          end
        end)
        |> Enum.reverse()
      end

      defp waraft_item_groups(buckets, shard_count) do
        0..(shard_count - 1)
        |> Enum.reduce([], fn shard_idx, acc ->
          case elem(buckets, shard_idx) do
            {[], 0} -> acc
            {items, count} -> [{shard_idx, Enum.reverse(items), count} | acc]
          end
        end)
        |> Enum.reverse()
      end

      defp collect_waraft_shard_batches(ctx, groups, count, submit_fun, _command_fun) do
        results =
          case groups do
            [{shard_idx, items, indices}] ->
              result = submit_fun.(shard_idx, items)

              merge_waraft_batch_results(
                ctx,
                shard_idx,
                indices,
                result,
                new_waraft_result_tuple(count)
              )

            _ ->
              groups
              |> Enum.map(fn {shard_idx, items, indices} ->
                {shard_idx, indices, async_waraft_shard_submit(shard_idx, items, submit_fun)}
              end)
              |> Enum.reduce(new_waraft_result_tuple(count), fn {shard_idx, indices, task}, acc ->
                result = await_waraft_shard_submit(task)
                merge_waraft_batch_results(ctx, shard_idx, indices, result, acc)
              end)
          end

        Tuple.to_list(results)
      end

      defp async_waraft_shard_submit(shard_idx, items, submit_fun) do
        trace_enabled? = Ferricstore.LatencyTrace.enabled?()

        Task.async(fn ->
          if trace_enabled? do
            previous_trace = Ferricstore.LatencyTrace.start(%{})

            try do
              result = submit_fun.(shard_idx, items)
              trace = Ferricstore.LatencyTrace.finish(previous_trace)
              {result, trace}
            catch
              kind, reason ->
                _ = Ferricstore.LatencyTrace.finish(previous_trace)
                :erlang.raise(kind, reason, __STACKTRACE__)
            end
          else
            submit_fun.(shard_idx, items)
          end
        end)
      end

      defp await_waraft_shard_submit(task) do
        case Task.await(task, 30_000) do
          {result, trace} when is_map(trace) ->
            Ferricstore.LatencyTrace.merge(trace)
            result

          result ->
            result
        end
      end

      defp collect_waraft_hot_shard_batches(ctx, groups, count, submit_async_fun, submit_sync_fun) do
        results =
          case groups do
            [{shard_idx, items, indices}] ->
              result = submit_sync_fun.(shard_idx, items)

              merge_waraft_hot_batch_results(
                ctx,
                shard_idx,
                indices,
                result,
                new_waraft_result_tuple(count)
              )

            _ ->
              {token_meta_pairs, results} =
                Enum.reduce(groups, {[], new_waraft_result_tuple(count)}, fn {shard_idx, items,
                                                                              indices},
                                                                             {tokens, acc} ->
                  {from, token} = ReplyAwaiter.new()

                  case submit_async_fun.(shard_idx, items, from) do
                    :ok ->
                      {[{token, {shard_idx, indices}} | tokens], acc}

                    {:direct, result} ->
                      {tokens,
                       merge_waraft_hot_batch_results(ctx, shard_idx, indices, result, acc)}

                    result ->
                      {tokens,
                       merge_waraft_hot_batch_results(ctx, shard_idx, indices, result, acc)}
                  end
                end)

              {_status, replies, _unresolved} =
                token_meta_pairs
                |> Enum.reverse()
                |> ReplyAwaiter.collect_tagged(30_000)

              Enum.reduce(replies, results, fn {{shard_idx, indices}, result}, acc ->
                merge_waraft_hot_batch_results(ctx, shard_idx, indices, result, acc)
              end)
          end

        Tuple.to_list(results)
      end

      defp collect_waraft_hot_shard_batch_status(_ctx, []), do: :ok

      defp collect_waraft_hot_shard_batch_status(ctx, [{shard_idx, items, count}]) do
        ctx
        |> merge_waraft_hot_batch_status(
          shard_idx,
          count,
          Ferricstore.Raft.Backend.write_put_batch(shard_idx, items)
        )
      end

      defp collect_waraft_hot_shard_batch_status(ctx, groups) do
        {token_meta_pairs, status} =
          Enum.reduce(groups, {[], :ok}, fn {shard_idx, items, count}, {tokens, status} ->
            {from, token} = ReplyAwaiter.new()

            case Ferricstore.Raft.WARaftBackend.write_put_batch_async(shard_idx, items, from) do
              :ok ->
                {[{token, {shard_idx, count}} | tokens], status}

              {:direct, result} ->
                {tokens,
                 combine_batch_status(
                   status,
                   merge_waraft_hot_batch_status(ctx, shard_idx, count, result)
                 )}

              result ->
                {tokens,
                 combine_batch_status(
                   status,
                   merge_waraft_hot_batch_status(ctx, shard_idx, count, result)
                 )}
            end
          end)

        {_status, replies, unresolved} =
          token_meta_pairs
          |> Enum.reverse()
          |> ReplyAwaiter.collect_tagged(30_000)

        status =
          if unresolved == [] do
            status
          else
            combine_batch_status(status, ErrorReasons.write_timeout_unknown())
          end

        Enum.reduce(replies, status, fn {{shard_idx, count}, result}, status ->
          combine_batch_status(
            status,
            merge_waraft_hot_batch_status(ctx, shard_idx, count, result)
          )
        end)
      end

      defp merge_waraft_hot_batch_results(ctx, shard_idx, indices, {:ok, values}, acc)
           when is_list(values) do
        case put_waraft_hot_batch_results(indices, values, acc) do
          {:ok, results, ok_count} ->
            if ok_count > 0 do
              bump_write_version(ctx, shard_idx, ok_count)
            end

            results

          {:error, _expected, _actual} ->
            merge_waraft_batch_results(
              ctx,
              shard_idx,
              indices,
              {:ok, values},
              acc
            )
        end
      end

      defp merge_waraft_hot_batch_results(ctx, shard_idx, indices, result, acc) do
        merge_waraft_batch_results(ctx, shard_idx, indices, result, acc)
      end

      defp merge_waraft_hot_batch_status(ctx, shard_idx, expected_count, {:ok, values})
           when is_list(values) do
        case batch_values_status(values, expected_count) do
          {:ok, ok_count} ->
            bump_write_version_if_needed(ctx, shard_idx, ok_count)
            :ok

          {:error, {:error, {:batch_result_mismatch, _, _}} = error, _ok_count} ->
            bump_write_version_if_needed(ctx, shard_idx, expected_count)
            error

          {:error, error, possible_write_count} ->
            bump_write_version_if_needed(ctx, shard_idx, possible_write_count)
            error
        end
      end

      defp merge_waraft_hot_batch_status(
             ctx,
             shard_idx,
             expected_count,
             {:ok, _invalid}
           ) do
        bump_write_version_if_needed(ctx, shard_idx, expected_count)
        ErrorReasons.write_timeout_unknown()
      end

      defp merge_waraft_hot_batch_status(
             ctx,
             shard_idx,
             expected_count,
             result
           )
           when result == {:error, {:timeout, :unknown_outcome}} or
                  result == {:error, :write_timeout_unknown} do
        bump_write_version_if_needed(ctx, shard_idx, expected_count)
        result
      end

      defp merge_waraft_hot_batch_status(_ctx, _shard_idx, _expected_count, {:error, _} = error),
        do: error

      defp merge_waraft_hot_batch_status(ctx, shard_idx, expected_count, :ok) do
        bump_write_version_if_needed(ctx, shard_idx, expected_count)
        :ok
      end

      defp merge_waraft_hot_batch_status(_ctx, _shard_idx, _expected_count, other), do: other

      defp bump_write_version_if_needed(_ctx, _shard_idx, 0), do: :ok

      defp bump_write_version_if_needed(ctx, shard_idx, count) do
        bump_write_version(ctx, shard_idx, count)
      end

      defp put_waraft_hot_batch_results(indices, values, acc) do
        put_waraft_hot_batch_results(indices, values, acc, 0, 0)
      end

      defp put_waraft_hot_batch_results([], [], acc, ok_count, _seen),
        do: {:ok, acc, ok_count}

      defp put_waraft_hot_batch_results([index | indices], [value | values], acc, ok_count, seen) do
        ok_count =
          if batch_write_may_have_applied?(value) do
            ok_count + 1
          else
            ok_count
          end

        put_waraft_hot_batch_results(
          indices,
          values,
          put_elem(acc, index, value),
          ok_count,
          seen + 1
        )
      end

      defp put_waraft_hot_batch_results(indices, values, _acc, _ok_count, seen) do
        {:error, seen + length(indices), seen + length(values)}
      end

      defp merge_waraft_batch_results(ctx, shard_idx, indices, result, acc) do
        {results, possible_write_count} =
          normalize_batch_write_result(result, length(indices))

        if possible_write_count > 0 do
          bump_write_version(ctx, shard_idx, possible_write_count)
        end

        indices
        |> Enum.zip(results)
        |> Enum.reduce(acc, fn {index, value}, results -> put_elem(results, index, value) end)
      end

      defp normalize_batch_write_result({:ok, values}, expected_count)
           when is_list(values) and is_integer(expected_count) and expected_count >= 0 do
        case count_exact_batch_possible_writes(values, expected_count, 0) do
          {:ok, possible_write_count} ->
            {values, possible_write_count}

          :mismatch ->
            unknown_batch_write_result(expected_count)
        end
      end

      defp normalize_batch_write_result({:ok, _invalid}, expected_count)
           when is_integer(expected_count) and expected_count >= 0 do
        unknown_batch_write_result(expected_count)
      end

      defp normalize_batch_write_result(result, expected_count)
           when is_integer(expected_count) and expected_count >= 0 do
        possible_write_count =
          if batch_write_may_have_applied?(result), do: expected_count, else: 0

        {List.duplicate(result, expected_count), possible_write_count}
      end

      defp count_exact_batch_possible_writes([], 0, possible_write_count),
        do: {:ok, possible_write_count}

      defp count_exact_batch_possible_writes([], _remaining, _possible_write_count),
        do: :mismatch

      defp count_exact_batch_possible_writes([_value | _rest], 0, _possible_write_count),
        do: :mismatch

      defp count_exact_batch_possible_writes(
             [value | rest],
             remaining,
             possible_write_count
           ) do
        count_exact_batch_possible_writes(
          rest,
          remaining - 1,
          if(batch_write_may_have_applied?(value),
            do: possible_write_count + 1,
            else: possible_write_count
          )
        )
      end

      defp unknown_batch_write_result(expected_count) do
        {List.duplicate(ErrorReasons.write_timeout_unknown(), expected_count), expected_count}
      end

      defp batch_write_may_have_applied?(result)
           when result == {:error, {:timeout, :unknown_outcome}} or
                  result == {:error, :write_timeout_unknown},
           do: true

      defp batch_write_may_have_applied?({:error, _reason}), do: false
      defp batch_write_may_have_applied?(_result), do: true

      defp zip_batch_groups_with_results(groups, results) when is_list(results) do
        if same_list_length?(groups, results) do
          Enum.zip(groups, results)
        else
          unknown = ErrorReasons.write_timeout_unknown()
          Enum.map(groups, &{&1, unknown})
        end
      end

      defp zip_batch_groups_with_results(groups, _invalid) do
        unknown = ErrorReasons.write_timeout_unknown()
        Enum.map(groups, &{&1, unknown})
      end

      defp same_list_length?([], []), do: true
      defp same_list_length?([_ | left], [_ | right]), do: same_list_length?(left, right)
      defp same_list_length?(_left, _right), do: false

      defp new_waraft_result_tuple(count) when is_integer(count) and count >= 0 do
        :erlang.make_tuple(count, ErrorReasons.write_timeout_unknown())
      end

      defp batch_quorum_put_status(_ctx, [], _origin_node), do: :ok

      defp batch_quorum_put_status(ctx, kv_pairs, origin_node) do
        case batch_put_admission(ctx, kv_pairs) do
          :ok ->
            cond do
              not durable_raft_ctx?(ctx) ->
                ctx
                |> local_batch_put_entries(kv_pairs)
                |> batch_results_status()

              selected_waraft_ctx?(ctx) and is_nil(origin_node) ->
                waraft_batch_put_entries_status(ctx, kv_pairs)

              true ->
                ctx
                |> do_ra_batch_quorum_put_entries(kv_pairs, origin_node)
                |> batch_results_status()
            end

          {:partitioned, admitted, admitted_indices, rejected, count} ->
            ctx
            |> execute_admitted_batch_put(admitted, origin_node)
            |> merge_batch_admission_results(admitted_indices, rejected, count)
            |> batch_results_status()
        end
      end

      defp batch_results_status(results) when is_list(results) do
        batch_results_status(results, :ok)
      end

      defp batch_results_status({:error, _reason} = error), do: error
      defp batch_results_status(:ok), do: :ok
      defp batch_results_status(other), do: other

      defp batch_results_status([], status), do: status
      defp batch_results_status([{:error, _reason} = error | _rest], _status), do: error
      defp batch_results_status([_ok | rest], status), do: batch_results_status(rest, status)

      defp batch_values_status(values, expected_count) do
        batch_values_status(values, expected_count, 0, 0, nil)
      end

      defp batch_values_status([], expected_count, seen, ok_count, nil)
           when seen == expected_count,
           do: {:ok, ok_count}

      defp batch_values_status([], expected_count, seen, ok_count, nil),
        do: {:error, {:error, {:batch_result_mismatch, expected_count, seen}}, ok_count}

      defp batch_values_status([], expected_count, seen, ok_count, first_error)
           when seen == expected_count,
           do: {:error, first_error, ok_count}

      defp batch_values_status([], expected_count, seen, ok_count, _first_error),
        do: {:error, {:error, {:batch_result_mismatch, expected_count, seen}}, ok_count}

      defp batch_values_status([value | rest], expected_count, seen, ok_count, first_error) do
        possible_write? = batch_write_may_have_applied?(value)

        first_error =
          case {first_error, value} do
            {nil, {:error, _reason} = error} -> error
            {existing, _value} -> existing
          end

        batch_values_status(
          rest,
          expected_count,
          seen + 1,
          if(possible_write?, do: ok_count + 1, else: ok_count),
          first_error
        )
      end

      defp combine_batch_status(:ok, next), do: next
      defp combine_batch_status({:error, _reason} = error, _next), do: error
      defp combine_batch_status(other, _next), do: other

      defp do_batch_quorum_put_entries(ctx, entries, origin_node) do
        if selected_waraft_ctx?(ctx) do
          waraft_batch_put_entries(ctx, entries)
        else
          do_ra_batch_quorum_put_entries(ctx, entries, origin_node)
        end
      end

      defp do_ra_batch_quorum_put_entries(ctx, entries, origin_node) do
        wv_size = :counters.info(ctx.write_version).size

        # Single pass: group final put_batch entries by shard. Public SET callers
        # pass {key, value}; internal callers may pass {key, value, expire_at_ms}.
        # We normalize while grouping so the hot path does not allocate old
        # per-key {:put, ...} commands or run a separate pre-map pass. Do not
        # re-expand this to generic commands unless a benchmark and apply-path audit
        # show the specialized term is no longer the faster shape.
        {by_shard, count, by_shard_entries} =
          entries
          |> Enum.reduce({%{}, 0, %{}}, fn entry, {shards, i, entries_map} ->
            {key, value, expire_at_ms} = normalize_put_batch_entry(entry)
            idx = shard_for(ctx, key)
            entry = Map.get(shards, idx, {[], []})
            {entries_acc, indices} = entry

            shards =
              Map.put(shards, idx, {[{key, value, expire_at_ms} | entries_acc], [i | indices]})

            entries_map =
              Map.update(entries_map, idx, [{key, value, expire_at_ms}], fn acc ->
                [{key, value, expire_at_ms} | acc]
              end)

            {shards, i + 1, entries_map}
          end)

        shard_refs =
          Enum.map(by_shard, fn {shard_idx, {entries, indices}} ->
            {from, token} = ReplyAwaiter.new()
            entries = Enum.reverse(entries)

            if origin_node == nil do
              Ferricstore.Raft.Batcher.write_put_batch(shard_idx, entries, from)
            else
              Ferricstore.Raft.Batcher.write_put_batch_forwarded(
                shard_idx,
                entries,
                from,
                origin_node
              )
            end

            {token, shard_idx, Enum.reverse(indices)}
          end)

        results =
          collect_shard_replies(
            shard_refs,
            wv_size,
            ctx,
            %{},
            System.monotonic_time(:millisecond)
          )

        # Per-shard not_leader → forward that shard's slice to its hinted leader.
        # Each shard reports independently; we re-issue just the failing shard.
        results =
          Enum.reduce(by_shard_entries, results, fn {shard_idx, entries}, acc ->
            # All indices for this shard share the same shard-level reply
            first_index = Map.get(by_shard, shard_idx) |> elem(1) |> List.last()

            case Map.get(acc, first_index) do
              {:error, {:not_leader, {_shard_name, leader_node}}} when is_atom(leader_node) ->
                merge_forwarded(acc, by_shard, shard_idx, entries, leader_node, ctx)

              {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
                merge_forwarded(acc, by_shard, shard_idx, entries, leader_node, ctx)

              _ ->
                acc
            end
          end)

        0..(count - 1)
        |> Enum.map(fn i -> Map.get(results, i, ErrorReasons.write_timeout_unknown()) end)
      end

      defp normalize_put_batch_entry({key, value}) when is_binary(key) and is_binary(value),
        do: {key, value, 0}

      defp normalize_put_batch_entry({key, value, expire_at_ms})
           when is_binary(key) and is_binary(value) and is_integer(expire_at_ms),
           do: {key, value, expire_at_ms}

      defp do_batch_quorum_delete_keys(ctx, keys, origin_node) do
        if selected_waraft_ctx?(ctx) do
          waraft_batch_delete_keys(ctx, keys)
        else
          do_ra_batch_quorum_delete_keys(ctx, keys, origin_node)
        end
      end

      defp do_ra_batch_quorum_delete_keys(ctx, keys, origin_node) do
        wv_size = :counters.info(ctx.write_version).size

        {by_shard, count, by_shard_keys} =
          keys
          |> Enum.reduce({%{}, 0, %{}}, fn key, {shards, i, keys_map} ->
            idx = shard_for(ctx, key)
            {keys_acc, indices} = Map.get(shards, idx, {[], []})

            {
              Map.put(shards, idx, {[key | keys_acc], [i | indices]}),
              i + 1,
              Map.update(keys_map, idx, [key], fn acc -> [key | acc] end)
            }
          end)

        shard_refs =
          Enum.map(by_shard, fn {shard_idx, {keys, indices}} ->
            {from, token} = ReplyAwaiter.new()
            keys = Enum.reverse(keys)

            if origin_node == nil do
              Ferricstore.Raft.Batcher.write_delete_batch(shard_idx, keys, from)
            else
              Ferricstore.Raft.Batcher.write_delete_batch_forwarded(
                shard_idx,
                keys,
                from,
                origin_node
              )
            end

            {token, shard_idx, Enum.reverse(indices)}
          end)

        results =
          collect_shard_replies(
            shard_refs,
            wv_size,
            ctx,
            %{},
            System.monotonic_time(:millisecond)
          )

        results =
          Enum.reduce(by_shard_keys, results, fn {shard_idx, keys}, acc ->
            {_keys, indices} = Map.fetch!(by_shard, shard_idx)
            first_index = List.last(indices)

            case Map.get(acc, first_index) do
              {:error, {:not_leader, {_shard_name, leader_node}}} when is_atom(leader_node) ->
                merge_forwarded_deletes(acc, by_shard, shard_idx, keys, leader_node, ctx)

              {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
                merge_forwarded_deletes(acc, by_shard, shard_idx, keys, leader_node, ctx)

              _ ->
                acc
            end
          end)

        0..(count - 1)
        |> Enum.map(fn i -> Map.get(results, i, ErrorReasons.write_timeout_unknown()) end)
      end

      defp merge_forwarded(acc, by_shard, shard_idx, entries, leader_node, ctx) do
        {_, indices} = Map.fetch!(by_shard, shard_idx)
        indices = Enum.reverse(indices)
        result = forward_batch_to_leader(ctx, leader_node, shard_idx, Enum.reverse(entries))
        {new_results, possible_write_count} = normalize_forwarded_batch_result(result, indices)
        bump_write_version_if_needed(ctx, shard_idx, possible_write_count)

        Enum.zip(indices, new_results)
        |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
      end

      defp merge_forwarded_commands(acc, by_shard, shard_idx, commands, leader_node, ctx) do
        {_, indices} = Map.fetch!(by_shard, shard_idx)
        indices = Enum.reverse(indices)

        result =
          forward_batch_commands_to_leader(ctx, leader_node, shard_idx, Enum.reverse(commands))

        {new_results, possible_write_count} = normalize_forwarded_batch_result(result, indices)
        bump_write_version_if_needed(ctx, shard_idx, possible_write_count)

        Enum.zip(indices, new_results)
        |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
      end

      defp merge_forwarded_deletes(acc, by_shard, shard_idx, keys, leader_node, ctx) do
        {_, indices} = Map.fetch!(by_shard, shard_idx)
        indices = Enum.reverse(indices)

        result = forward_delete_batch_to_leader(ctx, leader_node, shard_idx, Enum.reverse(keys))
        {new_results, possible_write_count} = normalize_forwarded_batch_result(result, indices)
        bump_write_version_if_needed(ctx, shard_idx, possible_write_count)

        Enum.zip(indices, new_results)
        |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
      end

      defp normalize_forwarded_batch_result(results, indices) when is_list(results) do
        normalize_batch_write_result({:ok, results}, length(indices))
      end

      defp normalize_forwarded_batch_result(result, indices) do
        normalize_batch_write_result(result, length(indices))
      end

      defp collect_shard_replies([], _wv_size, _ctx, acc, _start), do: acc

      defp collect_shard_replies(remaining_refs, wv_size, ctx, acc, start) do
        elapsed = System.monotonic_time(:millisecond) - start
        timeout = max(10_000 - elapsed, 0)

        {_status, replies, _unresolved} =
          remaining_refs
          |> Enum.map(fn {token, shard_idx, indices} -> {token, {shard_idx, indices}} end)
          |> ReplyAwaiter.collect_tagged(timeout)

        Enum.reduce(replies, acc, fn {{shard_idx, indices}, result}, next_acc ->
          apply_shard_results(result, indices, shard_idx, wv_size, ctx, next_acc)
        end)
      end

      defp apply_shard_results(result, indices, shard_idx, wv_size, ctx, acc) do
        {results, possible_write_count} =
          normalize_batch_write_result(result, length(indices))

        if possible_write_count > 0 and shard_idx < wv_size do
          :counters.add(ctx.write_version, shard_idx + 1, possible_write_count)
        end

        Enum.zip(indices, results)
        |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
      end
    end
  end
end
