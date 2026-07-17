defmodule Ferricstore.Store.Router.Part01 do
  @moduledoc false

  # Extracted from Router: valid_cold_file_ref .. get_with_file_ref
  defmacro __using__(_opts) do
    quote do
      @shard_batch_read_max_keys 512
      @shard_batch_read_max_key_size 65_535
      @shard_batch_read_max_key_bytes 1_048_576
      @shard_batch_read_deadline_ms 4_500
      @shard_batch_read_call_timeout_ms 5_000

      alias Ferricstore.CommandTime
      alias Ferricstore.ErrorReasons
      alias Ferricstore.ExpiryContext
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
      alias Ferricstore.Store.ExpiryTracker
      alias Ferricstore.Store.Keydir
      alias Ferricstore.Store.LFU
      alias Ferricstore.Store.ListOps
      alias Ferricstore.Store.ReadResult
      alias Ferricstore.Store.Router
      alias Ferricstore.Store.SlotMap
      alias Ferricstore.Store.TypeRegistry

      defguardp valid_cold_file_ref(file_id, value_size)
                when is_integer(file_id) and file_id >= 0 and is_integer(value_size) and
                       value_size >= 0

      defguardp valid_cold_location(file_id, offset, value_size)
                when valid_cold_file_ref(file_id, value_size) and is_integer(offset) and
                       offset >= 0

      defguardp valid_waraft_segment_location(file_id, offset, value_size)
                when is_tuple(file_id) and tuple_size(file_id) == 2 and
                       (elem(file_id, 0) == :waraft_segment or
                          elem(file_id, 0) == :waraft_projection or
                          elem(file_id, 0) == :waraft_apply_projection) and
                       is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0 and
                       is_integer(offset) and offset >= 0 and
                       is_integer(value_size) and value_size >= 0

      defguardp readable_cold_ref?(file_id, offset, value_size)
                when valid_cold_location(file_id, offset, value_size) or
                       (is_tuple(file_id) and tuple_size(file_id) == 2 and
                          elem(file_id, 0) == :flow_history and is_integer(elem(file_id, 1)) and
                          elem(file_id, 1) >= 0 and is_integer(offset) and offset >= 0 and
                          is_integer(value_size) and value_size >= 0) or
                       valid_waraft_segment_location(file_id, offset, value_size)

      defguardp valid_pending_value_size(value_size)
                when is_integer(value_size) and value_size >= 0

      defdelegate sweep_blob_garbage(ctx), to: Ferricstore.Store.Router.BlobGC

      # ---------------------------------------------------------------------------
      # Shard resolution helpers
      # ---------------------------------------------------------------------------

      @spec resolve_shard(FerricStore.Instance.t(), non_neg_integer()) :: atom()
      @doc false
      def resolve_shard(ctx, idx), do: elem(ctx.shard_names, idx)

      @doc false
      def safe_read_call(ctx, idx, request) do
        safe_read_call(ctx, idx, request, 5_000)
      end

      @doc false
      def safe_read_call(ctx, idx, request, timeout_ms) do
        {:ok, GenServer.call(resolve_shard(ctx, idx), request, timeout_ms)}
      catch
        :exit, {:noproc, _} ->
          emit_shard_unavailable(ctx, idx, request, :noproc)
          :unavailable

        :exit, {:timeout, _} ->
          emit_shard_unavailable(ctx, idx, request, :timeout)
          :unavailable
      end

      @doc false
      def read_shard_value(ctx, idx, key)
          when is_integer(idx) and idx >= 0 and is_binary(key) do
        keydir = resolve_keydir(ctx, idx)
        expiry_context = ExpiryContext.capture()

        case ets_get_full(ctx, idx, keydir, key, expiry_context) do
          {:hit, value, _lfu} ->
            {:ok, value}

          result when result in [:miss, :expired] ->
            {:ok, nil}

          :hlc_drift_exceeded ->
            ReadResult.failure(:hlc_drift_exceeded)

          _cold_or_unavailable ->
            if selected_waraft_ctx?(ctx) do
              {:ok, [value]} = do_batch_get_from_shard(ctx, idx, [key], :unlimited)

              case value do
                {:error, {:storage_read_failed, _reason}} = failure -> failure
                value -> {:ok, value}
              end
            else
              safe_read_call(ctx, idx, {:get, key})
            end
        end
      end

      @doc false
      def read_shard_values(ctx, idx, keys)
          when is_integer(idx) and idx >= 0 and is_list(keys) and
                 length(keys) <= @shard_batch_read_max_keys do
        if Enum.all?(keys, fn key ->
             is_binary(key) and byte_size(key) <= @shard_batch_read_max_key_size
           end) and
             Enum.reduce(keys, 0, fn key, total -> total + byte_size(key) end) <=
               @shard_batch_read_max_key_bytes do
          deadline_ms = System.monotonic_time(:millisecond) + @shard_batch_read_deadline_ms

          read_result =
            if selected_waraft_ctx?(ctx) do
              {:ok, values} = do_batch_get_from_shard(ctx, idx, keys, :unlimited)

              case ReadResult.first_failure(values) do
                nil -> {:ok, values}
                failure -> failure
              end
            else
              safe_read_call(
                ctx,
                idx,
                {:get_many, keys, deadline_ms},
                @shard_batch_read_call_timeout_ms
              )
            end

          case read_result do
            {:ok, values} when is_list(values) and length(values) == length(keys) ->
              {:ok, values}

            {:error, {:storage_read_failed, _reason}} = failure ->
              failure

            :unavailable ->
              :unavailable

            _invalid ->
              {:error, "ERR invalid shard batch read response"}
          end
        else
          {:error, "ERR invalid shard batch read request"}
        end
      end

      def read_shard_values(_ctx, _idx, _keys),
        do: {:error, "ERR invalid shard batch read request"}

      defp safe_write_call(ctx, idx, request) do
        GenServer.call(resolve_shard(ctx, idx), {:standalone_barrier_write, request})
      catch
        :exit, {:noproc, _} ->
          emit_shard_unavailable(ctx, idx, request, :noproc)
          {:error, "ERR shard not available"}

        :exit, {:timeout, _} ->
          emit_shard_unavailable(ctx, idx, request, :timeout)
          ErrorReasons.write_timeout_unknown()

        :exit, _reason ->
          emit_shard_unavailable(ctx, idx, request, :exit)
          {:error, "ERR shard not available"}
      end

      @doc false
      @spec with_key_latch(FerricStore.Instance.t(), binary(), (-> term())) :: term()
      def with_key_latch(ctx, key, fun) when is_binary(key) and is_function(fun, 0) do
        idx = shard_for(ctx, key)
        with_async_key_latch(ctx, idx, key, fun)
      end

      defp emit_shard_unavailable(ctx, idx, request, reason) do
        :telemetry.execute(
          [:ferricstore, :store, :shard_unavailable],
          %{count: 1},
          %{
            instance: ctx.name,
            shard_index: idx,
            request: request_name(request),
            reason: reason
          }
        )
      end

      defp request_name(request) when is_tuple(request), do: elem(request, 0)
      defp request_name(request), do: request

      @spec resolve_keydir(FerricStore.Instance.t(), non_neg_integer()) :: atom() | reference()
      @doc false
      def resolve_keydir(ctx, idx), do: elem(ctx.keydir_refs, idx)
      @spec effective_shard_count(FerricStore.Instance.t()) :: non_neg_integer()
      @doc false
      def effective_shard_count(ctx), do: ctx.shard_count

      # ---------------------------------------------------------------------------
      # Write-path dispatch: all durable writes go through the quorum/Raft path.
      # ---------------------------------------------------------------------------

      @spec quorum_write(FerricStore.Instance.t(), non_neg_integer(), tuple()) :: term()
      defp quorum_write(ctx, idx, command) do
        do_quorum_write(ctx, idx, command)
      end

      defp selected_waraft_ctx?(%{name: :default}), do: Ferricstore.Raft.Backend.running_waraft?()

      defp selected_waraft_ctx?(ctx) do
        # Default-instance routing must follow the backend pinned when the
        # application started. Custom test/spike instances are different: when
        # WARaftBackend.start(ctx) owns that exact context, writes for that context
        # must route to WARaft even if the default app is still running Ra.
        waraft_context_owner?(ctx)
      end

      defp waraft_context_owner?(ctx) do
        active_ctx = Ferricstore.Raft.WARaftBackend.context!(:ferricstore_waraft_backend)
        active_ctx.name == ctx.name and active_ctx.data_dir == ctx.data_dir
      catch
        _kind, _reason -> false
      end

      defp default_instance?(%{name: :default}), do: true
      defp default_instance?(_ctx), do: false

      defp durable_raft_ctx?(ctx), do: default_instance?(ctx) or selected_waraft_ctx?(ctx)

      defp do_quorum_write(ctx, idx, command) do
        if selected_waraft_ctx?(ctx) do
          result = Ferricstore.Raft.Backend.write(idx, command)

          case result do
            {:error, _} ->
              result

            _ ->
              bump_write_version(ctx, idx)
              result
          end
        else
          do_ra_quorum_write(ctx, idx, command)
        end
      end

      defp do_ra_quorum_write(ctx, idx, command) do
        result =
          try do
            Ferricstore.Raft.Batcher.write(idx, normalize_quorum_command(command))
          catch
            :exit, {:timeout, _} ->
              ErrorReasons.write_timeout_unknown()

            :exit, {:noproc, _} ->
              {:error, "ERR shard not available"}
          end

        case result do
          {:error, {:not_leader, {_shard, leader_node}}} when is_atom(leader_node) ->
            forward_to_leader(ctx, leader_node, idx, command)

          {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
            forward_to_leader(ctx, leader_node, idx, command)

          {:error, _} ->
            result

          _ ->
            size = :counters.info(ctx.write_version).size
            if idx < size, do: :counters.add(ctx.write_version, idx + 1, 1)
            result
        end
      end

      defp forward_to_leader(ctx, leader_node, idx, command) do
        if leader_node == node() do
          {:error, "ERR not leader, election in progress"}
        else
          forward_via_shard_call(ctx, leader_node, idx, command)
        end
      end

      defp forward_via_shard_call(_ctx, leader_node, idx, command) do
        try do
          remote_ctx = :erpc.call(leader_node, FerricStore.Instance, :get, [:default], 5_000)

          result =
            :erpc.call(
              leader_node,
              __MODULE__,
              :__forwarded_quorum_write__,
              [remote_ctx, idx, command, node()],
              10_000
            )

          case result do
            # Leader's batcher detected a cross-node caller and tagged the reply
            # with the ra_index. Barrier on local apply so reads on this node
            # see the just-written value (read-your-write across redirects).
            {:remote_applied_at, _ra_index, _real_result} ->
              barrier_forwarded_result(idx, result, 5_000)

            other ->
              barrier_forwarded_result(idx, other, 5_000)
          end
        catch
          _, reason ->
            if forward_timeout?(reason) do
              ErrorReasons.write_timeout_unknown()
            else
              {:error, "ERR leader unavailable"}
            end
        end
      end

      @doc false
      def __forwarded_quorum_write__(ctx, idx, command, origin_node) do
        forced_quorum_write(ctx, idx, command, origin_node)
      end

      @doc false
      def __barrier_forwarded_result__(idx, result, timeout_ms \\ 5_000),
        do: barrier_forwarded_result(idx, result, timeout_ms)

      @doc false
      @spec __forward_batch_failure_results__(term(), non_neg_integer()) :: [
              {:error, binary() | {:timeout, :unknown_outcome}}
            ]
      def __forward_batch_failure_results__(reason, count)
          when is_integer(count) and count >= 0 do
        List.duplicate(forward_failure_result(reason), count)
      end

      @spec forward_failure_result(term()) :: {:error, binary() | {:timeout, :unknown_outcome}}
      defp forward_failure_result(reason) do
        if forward_timeout?(reason) do
          ErrorReasons.write_timeout_unknown()
        else
          {:error, "ERR leader unavailable"}
        end
      end

      defp forward_timeout?({:erpc, :timeout}), do: true
      defp forward_timeout?(:timeout), do: true
      defp forward_timeout?({:timeout, _}), do: true
      defp forward_timeout?(_reason), do: false

      defp barrier_forwarded_result(idx, {:remote_applied_at, ra_index, real_result}, timeout_ms) do
        case Ferricstore.Raft.Batcher.await_local_applied(idx, ra_index, timeout_ms) do
          :ok -> real_result
          {:error, _reason} -> ErrorReasons.write_timeout_unknown()
        end
      end

      defp barrier_forwarded_result(_idx, other, _timeout_ms), do: other

      # Dispatches default-instance writes through quorum. Internal async
      # IO/WAL/read machinery remains separate from client acknowledgement policy.
      @spec raft_write(FerricStore.Instance.t(), non_neg_integer(), binary(), tuple()) :: term()
      defp raft_write(ctx, idx, key, command) do
        with :ok <- validate_flow_owned_write_locality(ctx, idx, key, command),
             {:ok, command} <- PolicyCommand.stamp(ctx, command) do
          cond do
            durable_raft_ctx?(ctx) ->
              quorum_write(ctx, idx, command)

            true ->
              # Custom embedded instances are local/direct. The default application
              # instance owns Raft durability.
              GenServer.call(
                elem(ctx.shard_names, idx),
                {:standalone_barrier_write, command}
              )
          end
        end
      end

      defp validate_flow_owned_write_locality(
             ctx,
             idx,
             key,
             {:set, key, _value, _expire_at_ms, opts}
           )
           when is_map(opts) do
        case Map.get(opts, :flow_retention_owner) do
          %{
            state_key: state_key
          }
          when is_binary(state_key) ->
            if extract_hash_tag(key) == extract_hash_tag(state_key) and
                 shard_for(ctx, state_key) == idx do
              :ok
            else
              {:error, "CROSSSLOT Flow-owned keys must hash to the owner shard"}
            end

          _not_flow_owned ->
            :ok
        end
      end

      defp validate_flow_owned_write_locality(_ctx, _idx, _key, _command), do: :ok

      defp forced_quorum_write(ctx, idx, command, origin_node) do
        do_forced_quorum_write(ctx, idx, command, origin_node)
      end

      defp do_forced_quorum_write(ctx, idx, command, origin_node) do
        if selected_waraft_ctx?(ctx) do
          do_quorum_write(ctx, idx, command)
        else
          do_ra_forced_quorum_write(ctx, idx, command, origin_node)
        end
      end

      defp do_ra_forced_quorum_write(ctx, idx, command, origin_node) do
        {from, token} = ReplyAwaiter.new()
        command = normalize_quorum_command(command)

        if origin_node == node() do
          Ferricstore.Raft.Batcher.write_async_quorum(idx, command, from)
        else
          Ferricstore.Raft.Batcher.write_async_quorum_forwarded(idx, command, from, origin_node)
        end

        result = ReplyAwaiter.await(token, 10_000, ErrorReasons.write_timeout_unknown())

        case result do
          # Local node isn't the leader for this shard. Forward via the same
          # path quorum_write uses so callers get a real result, not the
          # internal :not_leader error.
          {:error, {:not_leader, {_shard, leader_node}}} when is_atom(leader_node) ->
            forward_to_leader(ctx, leader_node, idx, command)

          {:error, {:not_leader, leader_node}} when is_atom(leader_node) ->
            forward_to_leader(ctx, leader_node, idx, command)

          {:error, _} ->
            result

          _ ->
            bump_write_version(ctx, idx)
            result
        end
      end

      defp normalize_quorum_command(
             {:compound_put, _redis_key, compound_key, value, expire_at_ms}
           ),
           do: {:compound_put, compound_key, value, expire_at_ms}

      defp normalize_quorum_command({:compound_delete, _redis_key, compound_key}),
        do: {:compound_delete, compound_key}

      defp normalize_quorum_command({:compound_delete_prefix, _redis_key, prefix}),
        do: {:compound_delete_prefix, prefix}

      defp normalize_quorum_command(command), do: command

      defp bump_write_version(%{write_version: write_version}, idx) do
        size = :counters.info(write_version).size
        if idx < size, do: :counters.add(write_version, idx + 1, 1)
        :ok
      end

      defp bump_write_version(_ctx, _idx), do: :ok

      defp bump_write_version(%{write_version: write_version}, idx, delta)
           when is_integer(delta) and delta > 0 do
        size = :counters.info(write_version).size
        if idx < size, do: :counters.add(write_version, idx + 1, delta)
        :ok
      end

      defp bump_write_version(_ctx, _idx, _delta), do: :ok

      defp with_async_key_latch(ctx, idx, key, fun) do
        case acquire_async_key_latches(ctx, [{idx, key}]) do
          {:ok, [{latch_tab, ^key}]} ->
            try do
              fun.()
            after
              release_async_key_latches([{latch_tab, key}])
            end

          {:error, {:timeout, wait_ms}} ->
            async_key_latch_timeout_error(wait_ms)
        end
      end

      defp acquire_async_key_latches(ctx, locks) do
        result =
          locks
          |> Enum.uniq()
          |> Enum.sort()
          |> Enum.reduce_while([], fn {idx, key}, held ->
            latch_tab = elem(ctx.latch_refs, idx)

            case wait_for_async_key_latch(latch_tab, idx, key) do
              :ok ->
                {:cont, [{latch_tab, key} | held]}

              {:error, reason} ->
                release_async_key_latches(held)
                {:halt, {:error, reason}}
            end
          end)

        case result do
          {:error, _reason} = error -> error
          held -> {:ok, Enum.reverse(held)}
        end
      end

      defp release_async_key_latches(held_latches) do
        Enum.each(held_latches, fn {latch_tab, key} ->
          :ets.select_delete(latch_tab, [{{key, self()}, [], [true]}])
        end)
      end

      defp wait_for_async_key_latch(latch_tab, idx, key) do
        case :ets.insert_new(latch_tab, {key, self()}) do
          true ->
            :ok

          false ->
            emit_async_key_latch_event(:blocked, idx, key, 0)

            wait_for_async_key_latch(
              latch_tab,
              idx,
              key,
              System.monotonic_time(:millisecond),
              async_key_latch_timeout_ms()
            )
        end
      end

      defp wait_for_async_key_latch(latch_tab, idx, key, started_ms, timeout_ms) do
        case :ets.insert_new(latch_tab, {key, self()}) do
          true ->
            :ok

          false ->
            wait_ms = max(System.monotonic_time(:millisecond) - started_ms, 0)

            if wait_ms >= timeout_ms do
              emit_async_key_latch_event(:timeout, idx, key, wait_ms)
              {:error, {:timeout, wait_ms}}
            else
              wait_for_async_key_latch_holder(latch_tab, idx, key, started_ms, timeout_ms)
            end
        end
      end

      defp wait_for_async_key_latch_holder(latch_tab, idx, key, started_ms, timeout_ms) do
        case :ets.lookup(latch_tab, key) do
          [{^key, holder}] when is_pid(holder) ->
            if Process.alive?(holder) do
              latch_retry_backoff()
            else
              :ets.select_delete(latch_tab, [{{key, holder}, [], [true]}])
            end

          _ ->
            latch_retry_backoff()
        end

        wait_for_async_key_latch(latch_tab, idx, key, started_ms, timeout_ms)
      end

      defp async_key_latch_timeout_ms do
        Application.get_env(
          :ferricstore,
          :router_async_key_latch_timeout_ms,
          @default_async_key_latch_timeout_ms
        )
      end

      defp async_key_latch_timeout_error(wait_ms) do
        {:error, async_key_latch_timeout_error_message(wait_ms)}
      end

      defp async_key_latch_timeout_error_message(wait_ms),
        do: "ERR write key latch timeout after #{wait_ms}ms"

      defp emit_async_key_latch_event(status, idx, key, wait_ms) do
        :telemetry.execute(
          [:ferricstore, :store, :async_key_latch],
          %{count: 1, wait_ms: wait_ms},
          %{status: status, shard_index: idx, redis_key_hash: :erlang.phash2(key)}
        )
      end

      defp latch_retry_backoff do
        receive do
        after
          1 -> :ok
        end
      end

      defp clear_compound_data_structure_for_string_put(ctx, idx, keydir, key) do
        if CompoundKey.internal_key?(key) do
          :ok
        else
          case compound_marker_for_string_put(ctx, idx, keydir, key) do
            :none ->
              :ok

            {:type, type_key, type} ->
              {_, file_path, _} = Ferricstore.Store.ActiveFile.get(ctx, idx)
              clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, type)
              delete_local_key(ctx, idx, keydir, file_path, type_key)
          end
        end
      end

      defp compound_marker_for_string_put(ctx, idx, keydir, key) do
        type_key = CompoundKey.type_key(key)

        case live_compound_marker(ctx, idx, keydir, type_key) do
          {:ok, type} ->
            {:type, type_key, type}

          :none ->
            :none
        end
      end

      # Keep ordinary string SET on the cheap path: a missing marker needs only
      # direct ETS lookup, while a present marker still uses origin_compound_get/4
      # so stale/expired/cold markers are handled correctly before clearing.
      defp live_compound_marker(ctx, idx, keydir, marker_key) do
        case :ets.lookup(keydir, marker_key) do
          [] ->
            :none

          _ ->
            case origin_compound_get(ctx, idx, keydir, marker_key) do
              nil -> :none
              marker -> {:ok, marker}
            end
        end
      end

      defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "hash"),
        do: delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.hash_prefix(key))

      defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "list") do
        delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.list_prefix(key))
        delete_local_key(ctx, idx, keydir, file_path, CompoundKey.list_meta_key(key))
      end

      defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "set"),
        do: delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.set_prefix(key))

      defp clear_compound_prefix_for_string_put(ctx, idx, keydir, file_path, key, "zset"),
        do: delete_local_prefix(ctx, idx, keydir, file_path, CompoundKey.zset_prefix(key))

      defp clear_compound_prefix_for_string_put(_ctx, _idx, _keydir, _file_path, _key, _type),
        do: :ok

      defp delete_local_prefix(ctx, idx, keydir, file_path, prefix) do
        Ferricstore.Store.Shard.ETS.prefix_each_key(keydir, prefix, fn compound_key ->
          delete_local_key(ctx, idx, keydir, file_path, compound_key)
        end)
      end

      defp delete_local_key(ctx, idx, keydir, file_path, key) do
        track_keydir_binary_delete(ctx, idx, keydir, key)
        :ets.delete(keydir, key)
        Ferricstore.Store.BitcaskWriter.delete(ctx, idx, file_path, key)
      end

      # -- Keydir binary memory tracking --
      # Only counts binaries > 64 bytes (refc binaries, off-heap).
      # Smaller binaries are inlined in the ETS tuple and counted by :ets.info(:memory).

      defp track_keydir_binary_insert(ctx, idx, keydir, key, new_val) do
        new_bytes = offheap_size(key) + offheap_size(new_val)

        old_bytes =
          case :ets.lookup(keydir, key) do
            [{^key, old_val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(old_val)
            _ -> 0
          end

        delta = new_bytes - old_bytes
        if delta != 0, do: :atomics.add(ctx.keydir_binary_bytes, idx + 1, delta)
      end

      defp track_keydir_binary_delete(ctx, idx, keydir, key) do
        bytes =
          case :ets.lookup(keydir, key) do
            [{^key, val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(val)
            _ -> 0
          end

        if bytes > 0, do: :atomics.sub(ctx.keydir_binary_bytes, idx + 1, bytes)
      end

      defp track_keydir_binary_delete_known(ctx, idx, key, value) do
        bytes = offheap_size(key) + offheap_size(value)
        if bytes > 0, do: :atomics.sub(ctx.keydir_binary_bytes, idx + 1, bytes)
      end

      defp delete_observed_keydir_entry(
             ctx,
             idx,
             keydir,
             {key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size} = entry
           ) do
        if Keydir.delete_exact(keydir, entry) do
          track_keydir_binary_delete_known(ctx, idx, key, value)
          ExpiryTracker.adjust(ctx, idx, expire_at_ms, 0)
          true
        else
          false
        end
      end

      defp offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
      defp offheap_size(_), do: 0

      defp stored_value_size(value) when is_binary(value), do: byte_size(value)

      defp stored_value_size(value) when is_integer(value),
        do: byte_size(Integer.to_string(value))

      defp stored_value_size(value) when is_float(value), do: byte_size(Float.to_string(value))
      defp stored_value_size(value), do: value |> to_string() |> byte_size()

      defp cold_logical_value_size(ctx, idx, key, file_id, offset, value_size) do
        if blob_ref_candidate?(ctx, value_size) do
          path = cold_file_path(ctx, idx, file_id)

          case read_cold_async(path, offset, key) do
            {:ok, value} ->
              case BlobRef.decode(value) do
                {:ok, %BlobRef{size: size}} -> size
                :error -> value_size
              end

            {:error, reason} ->
              ReadResult.failure({:cold_read_failed, reason})
          end
        else
          value_size
        end
      end

      defp waraft_logical_value_size(ctx, idx, key, file_id, value_size) do
        if blob_ref_candidate?(ctx, value_size) do
          case Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                 ctx,
                 idx,
                 file_id,
                 key
               ) do
            {:ok, value} ->
              case BlobRef.decode(value) do
                {:ok, %BlobRef{size: size}} -> size
                :error -> value_size
              end

            :not_found ->
              ReadResult.failure({:cold_read_failed, :missing_live_cold_entry})

            {:error, reason} ->
              emit_waraft_segment_read_error(ctx, idx, file_id, reason, 1)
              ReadResult.failure({:cold_read_failed, reason})
          end
        else
          value_size
        end
      end

      # -------------------------------------------------------------------
      # Routing helpers
      # -------------------------------------------------------------------

      @doc """
      Returns the slot (0-1023) for a key, respecting hash tags.
      """
      @spec slot_for(FerricStore.Instance.t(), binary()) :: non_neg_integer()
      def slot_for(_ctx, key) do
        SlotMap.slot_for_key(key)
      end

      @doc """
      Returns the shard index (0-based) that owns `key`.

      Routes through the 1,024-slot indirection layer:
      `key -> SlotMap.slot_for_key/1 -> slot_map[slot] -> shard_index`

      Supports Redis hash tags: if the key contains `{tag}` (non-empty content
      between the first `{` and the next `}`), the tag is used for hashing
      instead of the full key.
      """
      @spec shard_for(FerricStore.Instance.t(), binary()) :: non_neg_integer()
      def shard_for(ctx, key) do
        slot = slot_for(ctx, key)
        elem(ctx.slot_map, slot)
      end

      @doc """
      Extracts the hash tag from a key, following Redis hash tag semantics.

      If the key contains a substring enclosed in `{...}` where the content
      between the first `{` and the next `}` is non-empty, that substring is
      used for hashing instead of the full key. This allows related keys to
      be routed to the same shard.

      ## Examples

          iex> Ferricstore.Store.Router.extract_hash_tag("{user:42}:session")
          "user:42"

          iex> Ferricstore.Store.Router.extract_hash_tag("no_tag")
          nil

          iex> Ferricstore.Store.Router.extract_hash_tag("{}empty")
          nil

      """
      @spec extract_hash_tag(binary()) :: binary() | nil
      def extract_hash_tag(key) do
        case :binary.match(key, "{") do
          {start, 1} ->
            rest_start = start + 1
            rest_len = byte_size(key) - rest_start

            case :binary.match(key, "}", [{:scope, {rest_start, rest_len}}]) do
              {end_pos, 1} when end_pos > rest_start ->
                binary_part(key, rest_start, end_pos - rest_start)

              _ ->
                nil
            end

          :nomatch ->
            nil
        end
      end

      @doc """
      Returns the registered process name for the shard at `index`.

      Uses the pre-computed tuple from the instance context for O(1) lookup.
      """
      @spec shard_name(FerricStore.Instance.t(), non_neg_integer()) :: atom()
      def shard_name(ctx, index), do: elem(ctx.shard_names, index)

      @doc """
      Returns the keydir ETS table ref for the shard at `index`.

      Uses the pre-computed tuple from the instance context for O(1) lookup.
      """
      @spec keydir_name(FerricStore.Instance.t(), non_neg_integer()) :: atom() | reference()
      def keydir_name(ctx, index), do: elem(ctx.keydir_refs, index)

      # -------------------------------------------------------------------
      # Convenience accessors (dispatch to correct shard)
      # -------------------------------------------------------------------

      @doc """
      Returns the on-disk file reference for a key's value, or `nil`.

      Used by the sendfile optimisation in standalone TCP mode. Returns
      `{file_path, value_byte_offset, value_size}` for cold (on-disk) keys.
      Returns `nil` for hot keys (ETS), expired keys, or missing keys --
      the caller should fall back to the normal read path.

      Only cold keys benefit from sendfile: hot keys are already in BEAM memory
      and would need a normal `get` + `transport.send`.
      """
      @spec get_file_ref(FerricStore.Instance.t(), binary()) ::
              {binary(), non_neg_integer(), non_neg_integer()} | nil | ReadResult.failure()
      def get_file_ref(ctx, key) do
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)
        expiry_context = ExpiryContext.capture()

        case ets_get_full(ctx, idx, keydir, key, expiry_context) do
          {:hit, _value, _lfu} ->
            # Hot key — value is in ETS, sendfile not applicable.
            nil

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) ->
            path = cold_file_path(ctx, idx, file_id)

            case file_ref_from_cold_location(ctx, idx, path, offset, key, value_size, true) do
              {:ok, {file_ref_path, value_offset, size}} ->
                Stats.record_cold_read(ctx, key)
                {file_ref_path, value_offset, size}

              nil ->
                case retry_changed_file_ref(
                       ctx,
                       idx,
                       keydir,
                       key,
                       {file_id, offset, value_size},
                       expiry_context
                     ) do
                  {:cold_ref, retry_path, value_offset, retry_size} ->
                    Stats.record_cold_read(ctx, key)
                    {retry_path, value_offset, retry_size}

                  {:error, {:storage_read_failed, _reason}} = failure ->
                    failure

                  _ ->
                    record_keyspace_miss(ctx, key)
                    nil
                end
            end

          {:cold, _file_id, _offset, _value_size} ->
            # Invalid file ref — fall back to GenServer.
            case safe_read_call(ctx, idx, {:get_file_ref, key}) do
              {:ok, {path, value_offset, value_size} = result}
              when is_binary(path) and is_integer(value_offset) and is_integer(value_size) ->
                Stats.record_cold_read(ctx, key)
                result

              {:ok, nil} ->
                record_keyspace_miss(ctx, key)
                nil

              {:ok, _other} ->
                nil

              :unavailable ->
                ReadResult.failure(:shard_unavailable)
            end

          {:invalid, entry} ->
            ReadResult.failure({:invalid_keydir_entry, entry})

          :hlc_drift_exceeded ->
            ReadResult.failure(:hlc_drift_exceeded)

          :expired ->
            record_keyspace_miss(ctx, key)
            nil

          :miss ->
            # Key doesn't exist. No GenServer needed.
            record_keyspace_miss(ctx, key)
            nil

          :no_table ->
            ReadResult.failure(:keydir_unavailable)
        end
      end

      @doc """
      Unified GET that returns everything from a single ETS lookup.

      Returns:
        - `{:hot, value}` — value is in ETS, ready to return
        - `{:cold_ref, path, offset, size}` — value is on disk, file ref for sendfile
        - `{:cold_value, value}` — value was on disk, GenServer fetched it
        - `:miss` — key doesn't exist
      """
      @spec get_with_file_ref(FerricStore.Instance.t(), binary()) ::
              {:hot, binary()}
              | {:cold_ref, binary(), non_neg_integer(), non_neg_integer()}
              | {:cold_value, binary()}
              | {:error, binary()}
              | ReadResult.failure()
              | :miss
      def get_with_file_ref(ctx, key) do
        do_get_with_file_ref(ctx, key, true)
      end
    end
  end
end
