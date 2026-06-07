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
        defp sampled_read_bookkeeping_batch(_ctx, [], _max_hits), do: :ok
      
        defp sampled_read_bookkeeping_batch(ctx, hot_hits, max_hits)
             when is_list(hot_hits) and is_integer(max_hits) and max_hits >= 0 do
          if Stats.cache_tracking_enabled?() do
            hot_hits =
              Enum.filter(hot_hits, fn {_keydir, key, _lfu} -> Stats.cache_tracking_key?(key) end)
      
            max_hits = length(hot_hits)
            sample_state = Stats.start_keyspace_hit_batch(ctx, max_hits)
            touched = sampled_hit_entries(sample_state, hot_hits)
      
            :ok = Stats.finish_keyspace_hit_batch(ctx, finish_hit_batch_state(sample_state, max_hits))
      
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
          warm_ets_after_cold_read(ctx, keydir_index(ctx, keydir), keydir, key, value, file_id, offset)
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
                [{^key, nil, expire_at_ms, _old_lfu, ^file_id, ^offset, value_size}] ->
                  :ets.insert(keydir, {key, value, expire_at_ms, lfu, file_id, offset, value_size})
                  track_keydir_binary_warm(ctx, idx, value)
      
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
      
        @doc """
        Batch PUT API with `:ok | {:error, _}` result shape.
      
        The default application instance submits through quorum. Embedded/custom
        instances write locally because the Raft batchers are owned by the default
        application instance.
        """
        @spec batch_put(FerricStore.Instance.t(), [{binary(), binary()}]) ::
                :ok | {:error, binary()}
        def batch_put(%{name: name} = ctx, kv_pairs) when name != :default do
          case pressured_batch_shard(ctx, kv_pairs) do
            nil -> batch_local_put(ctx, kv_pairs)
            idx -> {:error, "ERR disk pressure on shard #{idx}, rejecting write"}
          end
        end
      
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
      
        defp batch_local_put(ctx, kv_pairs) do
          # Embedded/custom instances are local/direct. They must not consult the
          # default instance's Raft batchers; those global names are owned by the
          # application instance and can be under unrelated backpressure.
          Enum.reduce_while(kv_pairs, :ok, fn {key, value}, :ok ->
            case put(ctx, key, value, 0) do
              :ok -> {:cont, :ok}
              {:error, _} = err -> {:halt, err}
              other -> {:halt, {:error, inspect(other)}}
            end
          end)
        end
      
        defp pressured_batch_shard(ctx, kv_pairs) do
          Enum.find_value(kv_pairs, fn {key, _value} ->
            idx = shard_for(ctx, key)
            if shard_under_disk_pressure?(ctx, idx), do: idx, else: nil
          end)
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
      
        defp batch_quorum_commands(ctx, keyed_commands) do
          batch_quorum_commands(ctx, keyed_commands, nil)
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
              {:put, entries} -> do_batch_quorum_put_entries(ctx, entries, nil)
              {:delete, keys} -> do_batch_quorum_delete_keys(ctx, keys, nil)
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
      
        defp direct_batch_command_shape(_commands, _mode, _acc), do: :generic
      
        defp batch_quorum_commands(_ctx, [], _origin_node), do: []
      
        defp batch_quorum_commands(ctx, keyed_commands, origin_node) do
          do_batch_quorum_commands(ctx, keyed_commands, origin_node)
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
            collect_shard_replies(shard_refs, wv_size, ctx, %{}, System.monotonic_time(:millisecond))
      
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
          cond do
            not durable_raft_ctx?(ctx) ->
              local_batch_put_entries(ctx, kv_pairs)
      
            true ->
              do_batch_quorum_put_entries(ctx, kv_pairs, origin_node)
          end
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
                                                                                           {buckets, i} ->
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
      
        defp waraft_batch_delete_keys(_ctx, []), do: []
      
        defp waraft_batch_delete_keys(ctx, keys) do
          {buckets, count} =
            Enum.reduce(keys, {new_waraft_batch_buckets(ctx.shard_count), 0}, fn key, {buckets, i} ->
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
                  {shard_idx, indices, Task.async(fn -> submit_fun.(shard_idx, items) end)}
                end)
                |> Enum.reduce(new_waraft_result_tuple(count), fn {shard_idx, indices, task}, acc ->
                  result = Task.await(task, 30_000)
                  merge_waraft_batch_results(ctx, shard_idx, indices, result, acc)
                end)
            end
      
          Tuple.to_list(results)
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
                        {tokens, merge_waraft_hot_batch_results(ctx, shard_idx, indices, result, acc)}
      
                      result ->
                        {tokens, merge_waraft_hot_batch_results(ctx, shard_idx, indices, result, acc)}
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
      
        defp merge_waraft_hot_batch_results(ctx, shard_idx, indices, {:ok, values}, acc)
             when is_list(values) do
          case put_waraft_hot_batch_results(indices, values, acc) do
            {:ok, results, ok_count} ->
              if ok_count > 0 do
                bump_write_version(ctx, shard_idx, ok_count)
              end
      
              results
      
            {:error, expected, actual} ->
              merge_waraft_batch_results(
                ctx,
                shard_idx,
                indices,
                {:error, {:batch_result_mismatch, expected, actual}},
                acc
              )
          end
        end
      
        defp merge_waraft_hot_batch_results(ctx, shard_idx, indices, result, acc) do
          merge_waraft_batch_results(ctx, shard_idx, indices, result, acc)
        end
      
        defp put_waraft_hot_batch_results(indices, values, acc) do
          put_waraft_hot_batch_results(indices, values, acc, 0, 0)
        end
      
        defp put_waraft_hot_batch_results([], [], acc, ok_count, _seen),
          do: {:ok, acc, ok_count}
      
        defp put_waraft_hot_batch_results([index | indices], [value | values], acc, ok_count, seen) do
          ok_count =
            if value == :ok or not match?({:error, _}, value) do
              ok_count + 1
            else
              ok_count
            end
      
          put_waraft_hot_batch_results(indices, values, put_elem(acc, index, value), ok_count, seen + 1)
        end
      
        defp put_waraft_hot_batch_results(indices, values, _acc, _ok_count, seen) do
          {:error, seen + length(indices), seen + length(values)}
        end
      
        defp merge_waraft_batch_results(ctx, shard_idx, indices, result, acc) do
          results =
            case result do
              {:ok, values} when is_list(values) -> values
              {:error, _} = error -> List.duplicate(error, length(indices))
              other -> List.duplicate(other, length(indices))
            end
      
          ok_count = Enum.count(results, fn value -> value == :ok or not match?({:error, _}, value) end)
      
          if ok_count > 0 do
            bump_write_version(ctx, shard_idx, ok_count)
          end
      
          indices
          |> Enum.zip(results)
          |> Enum.reduce(acc, fn {index, value}, results -> put_elem(results, index, value) end)
        end
      
        defp new_waraft_result_tuple(count) when is_integer(count) and count >= 0 do
          :erlang.make_tuple(count, ErrorReasons.write_timeout_unknown())
        end
      
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
              shards = Map.put(shards, idx, {[{key, value, expire_at_ms} | entries_acc], [i | indices]})
      
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
            collect_shard_replies(shard_refs, wv_size, ctx, %{}, System.monotonic_time(:millisecond))
      
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
            collect_shard_replies(shard_refs, wv_size, ctx, %{}, System.monotonic_time(:millisecond))
      
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
          new_results = forward_batch_to_leader(ctx, leader_node, shard_idx, Enum.reverse(entries))
      
          Enum.zip(indices, new_results)
          |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
        end
      
        defp merge_forwarded_commands(acc, by_shard, shard_idx, commands, leader_node, ctx) do
          {_, indices} = Map.fetch!(by_shard, shard_idx)
          indices = Enum.reverse(indices)
      
          new_results =
            forward_batch_commands_to_leader(ctx, leader_node, shard_idx, Enum.reverse(commands))
      
          Enum.zip(indices, new_results)
          |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
        end
      
        defp merge_forwarded_deletes(acc, by_shard, shard_idx, keys, leader_node, ctx) do
          {_, indices} = Map.fetch!(by_shard, shard_idx)
          indices = Enum.reverse(indices)
          new_results = forward_delete_batch_to_leader(ctx, leader_node, shard_idx, Enum.reverse(keys))
      
          Enum.zip(indices, new_results)
          |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
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
      
        defp apply_shard_results({:ok, results}, indices, shard_idx, wv_size, ctx, acc)
             when is_list(results) do
          ok_count = Enum.count(results, fn r -> r == :ok or not match?({:error, _}, r) end)
      
          if ok_count > 0 and shard_idx < wv_size do
            :counters.add(ctx.write_version, shard_idx + 1, ok_count)
          end
      
          Enum.zip(indices, results)
          |> Enum.reduce(acc, fn {i, r}, a -> Map.put(a, i, r) end)
        end
      
        defp apply_shard_results(result, indices, shard_idx, wv_size, ctx, acc) do
          case result do
            {:error, _} ->
              Enum.reduce(indices, acc, fn i, a -> Map.put(a, i, result) end)
      
            _ ->
              if shard_idx < wv_size,
                do: :counters.add(ctx.write_version, shard_idx + 1, length(indices))
      
              Enum.reduce(indices, acc, fn i, a -> Map.put(a, i, result) end)
          end
        end
    end
  end
end
