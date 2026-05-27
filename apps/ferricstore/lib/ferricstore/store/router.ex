defmodule Ferricstore.Store.Router do
  @moduledoc """
  Routes keys to shard GenServers using the shared `Ferricstore.Store.SlotMap`
  hashing implementation.

  This is a pure module with no process state. It provides two categories of
  functions:

  1. **Routing helpers** -- `shard_for/2` and `shard_name/2` map a key to its
     owning shard index and registered process name respectively. Supports
     Redis hash tags: keys containing `{tag}` are hashed on the tag content,
     allowing related keys to co-locate on the same shard.

  2. **Convenience accessors** -- `get/2`, `put/4`, `delete/2`, `exists?/2`,
     `keys/1`, and `dbsize/1` dispatch to the correct shard GenServer
     transparently.

  All public functions take a `ctx` (`FerricStore.Instance.t()`) as the first
  argument, replacing all persistent_term lookups with instance-local state.
  """

  alias Ferricstore.CommandTime
  alias Ferricstore.HLC
  alias Ferricstore.HyperLogLog, as: HLL
  alias Ferricstore.ErrorReasons
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Raft.PerfToggles
  alias Ferricstore.Raft.ReplyAwaiter
  alias Ferricstore.Stats

  alias Ferricstore.Store.{
    BlobRef,
    BlobStore,
    BlobValue,
    CompoundCommand,
    CompoundKey,
    LFU,
    ListOps,
    SlotMap,
    TypeRegistry
  }

  @cold_batch_read_timeout_ms 10_000
  @cold_location_retry_attempts 8
  @cold_location_retry_sleep_ms 1
  @default_async_key_latch_timeout_ms 30_000
  @flow_claim_cursor_table :ferricstore_flow_claim_due_any_cursor
  @flow_claim_due_any_window_multiplier 8
  @flow_claim_due_precheck_slack_ms 5
  @flow_shard_marker :__flow_shard_index__

  defguardp valid_cold_file_ref(file_id, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(value_size) and
                   value_size >= 0

  defguardp valid_cold_location(file_id, offset, value_size)
            when valid_cold_file_ref(file_id, value_size) and is_integer(offset) and offset >= 0

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

  @doc false
  @spec sweep_blob_garbage(FerricStore.Instance.t()) ::
          {:ok, map()} | {:error, term()}
  def sweep_blob_garbage(ctx) do
    initial = blob_gc_empty_stats()

    result =
      0..(effective_shard_count(ctx) - 1)
      |> Enum.reduce_while({:ok, initial}, fn idx, {:ok, acc} ->
        case sweep_blob_garbage_shard(ctx, idx) do
          {:ok, %{skipped: true} = stats} ->
            {:halt, {:ok, Map.merge(blob_gc_empty_stats(), stats)}}

          {:ok, stats} ->
            {:cont, {:ok, blob_gc_merge_stats(acc, stats)}}

          {:error, reason} ->
            emit_blob_gc_failed(ctx, idx, reason)
            {:halt, {:error, {idx, reason}}}
        end
      end)

    case result do
      {:ok, stats} ->
        emit_blob_gc(ctx, stats)
        {:ok, stats}

      {:error, _reason} = error ->
        error
    end
  end

  defp sweep_blob_garbage_shard(ctx, idx) do
    state = :sys.get_state(resolve_shard(ctx, idx))

    with :ok <- blob_gc_replay_safe?(state, idx),
         :ok <- blob_gc_fsync_active_file(state),
         {:ok, stats} <-
           BlobStore.sweep_unreferenced_with_live_refs(ctx.data_dir, idx, fn ->
             with {:ok, live_refs} <- blob_gc_live_refs(ctx, idx, state),
                  :ok <- blob_gc_after_live_refs_hook(ctx, idx, live_refs) do
               {:ok, live_refs}
             end
           end) do
      {:ok, Map.merge(blob_gc_empty_stats(), stats)}
    end
  catch
    :exit, reason -> {:error, {:blob_gc_shard_unavailable, reason}}
  end

  defp blob_gc_replay_safe?(%{raft?: true, instance_ctx: instance_ctx}, idx) do
    with %{last_applied_index: applied_ref, last_released_cursor_index: released_ref} <-
           instance_ctx,
         {:ok, applied} <- blob_gc_read_replay_index(applied_ref, idx),
         {:ok, released} <- blob_gc_read_replay_index(released_ref, idx) do
      if applied > released do
        {:ok,
         %{
           deleted_files: 0,
           deleted_bytes: 0,
           kept_files: 0,
           deleted_tmp_files: 0,
           deleted_tmp_bytes: 0,
           skipped: true,
           reason: {:raft_replay_gap, applied, released}
         }}
      else
        :ok
      end
    else
      _ ->
        {:ok,
         %{
           deleted_files: 0,
           deleted_bytes: 0,
           kept_files: 0,
           deleted_tmp_files: 0,
           deleted_tmp_bytes: 0,
           skipped: true,
           reason: :missing_raft_replay_metrics
         }}
    end
  end

  defp blob_gc_replay_safe?(_state, _idx), do: :ok

  defp blob_gc_read_replay_index(ref, idx) do
    value = :atomics.get(ref, idx + 1)

    if is_integer(value) and value >= 0 do
      {:ok, value}
    else
      :error
    end
  rescue
    ArgumentError -> :error
  catch
    :exit, _ -> :error
  end

  defp blob_gc_fsync_active_file(%{active_file_path: path}) when is_binary(path) do
    case Ferricstore.Bitcask.NIF.v2_fsync(path) do
      :ok -> :ok
      {:ok, _} -> :ok
      {:error, reason} -> {:error, {:blob_gc_active_fsync_failed, path, reason}}
      other -> {:error, {:blob_gc_active_fsync_failed, path, other}}
    end
  end

  defp blob_gc_fsync_active_file(_state),
    do: {:error, {:blob_gc_active_fsync_failed, nil, :missing}}

  defp blob_gc_live_refs(ctx, idx, state) do
    keydir = Map.get(state, :ets) || resolve_keydir(ctx, idx)

    keydir
    |> :ets.tab2list()
    |> Enum.reduce_while({:ok, MapSet.new()}, fn entry, {:ok, refs} ->
      case blob_gc_entry_ref(ctx, idx, state, entry) do
        {:ok, nil} -> {:cont, {:ok, refs}}
        {:ok, %BlobRef{} = ref} -> {:cont, {:ok, MapSet.put(refs, ref)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp blob_gc_entry_ref(_ctx, _idx, _state, {key, value, _exp, _lfu, _fid, _off, _size})
       when is_binary(key) and is_binary(value) do
    blob_gc_decode_ref(value)
  end

  defp blob_gc_entry_ref(ctx, idx, state, {key, _value, _exp, _lfu, fid, off, _size})
       when is_binary(key) and is_integer(fid) and fid >= 0 and is_integer(off) and off >= 0 do
    path = blob_gc_entry_file_path(ctx, idx, state, key, fid)

    case Ferricstore.Store.ColdRead.pread_at(path, off, key, @cold_batch_read_timeout_ms) do
      {:ok, value} -> blob_gc_decode_ref(value)
      {:error, reason} -> {:error, {:blob_gc_live_ref_scan_failed, key, reason}}
    end
  end

  defp blob_gc_entry_ref(_ctx, _idx, _state, _entry), do: {:ok, nil}

  defp blob_gc_after_live_refs_hook(ctx, idx, live_refs) do
    case Process.get(:ferricstore_blob_gc_after_live_refs_hook) do
      fun when is_function(fun, 3) -> fun.(ctx, idx, live_refs)
      _other -> :ok
    end
  end

  defp blob_gc_decode_ref(value) when is_binary(value) do
    case BlobRef.decode(value) do
      {:ok, %BlobRef{} = ref} -> {:ok, ref}
      _ -> {:ok, nil}
    end
  end

  defp blob_gc_entry_file_path(ctx, idx, state, key, fid) do
    redis_key = CompoundKey.extract_redis_key(key)

    case Map.get(Map.get(state, :promoted_instances, %{}), redis_key) do
      %{path: dedicated_path} when is_binary(dedicated_path) ->
        Ferricstore.Store.Shard.Compound.dedicated_file_path(dedicated_path, fid)

      _ ->
        cold_file_path(ctx, idx, fid)
    end
  end

  defp blob_gc_empty_stats do
    %{
      deleted_files: 0,
      deleted_bytes: 0,
      kept_files: 0,
      deleted_tmp_files: 0,
      deleted_tmp_bytes: 0
    }
  end

  defp blob_gc_merge_stats(acc, stats) do
    Map.merge(acc, stats, fn _key, a, b when is_integer(a) and is_integer(b) -> a + b end)
  end

  defp emit_blob_gc(ctx, stats) do
    :telemetry.execute([:ferricstore, :blob, :gc], stats, %{
      instance: ctx.name,
      shard_count: effective_shard_count(ctx),
      result: if(Map.get(stats, :skipped), do: :skipped, else: :ok),
      reason: Map.get(stats, :reason)
    })
  end

  defp emit_blob_gc_failed(ctx, idx, reason) do
    :telemetry.execute(
      [:ferricstore, :blob, :gc, :failed],
      %{count: 1},
      %{instance: ctx.name, shard_index: idx, reason: reason}
    )
  end

  # ---------------------------------------------------------------------------
  # Shard resolution helpers
  # ---------------------------------------------------------------------------

  @spec resolve_shard(FerricStore.Instance.t(), non_neg_integer()) :: atom()
  @doc false
  def resolve_shard(ctx, idx), do: elem(ctx.shard_names, idx)

  @doc false
  def safe_read_call(ctx, idx, request) do
    {:ok, GenServer.call(resolve_shard(ctx, idx), request)}
  catch
    :exit, {:noproc, _} ->
      emit_shard_unavailable(ctx, idx, request, :noproc)
      :unavailable

    :exit, {:timeout, _} ->
      emit_shard_unavailable(ctx, idx, request, :timeout)
      :unavailable
  end

  defp safe_write_call(ctx, idx, request) do
    GenServer.call(resolve_shard(ctx, idx), request)
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
  def __forward_batch_failure_results__(reason, count) when is_integer(count) and count >= 0 do
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
  defp raft_write(ctx, idx, _key, command) do
    cond do
      durable_raft_ctx?(ctx) ->
        quorum_write(ctx, idx, command)

      true ->
        # Custom embedded instances are local/direct. The default application
        # instance owns Raft durability.
        GenServer.call(elem(ctx.shard_names, idx), command)
    end
  end

  defp shard_under_disk_pressure?(ctx, idx) do
    size = :atomics.info(ctx.disk_pressure).size
    idx < size and :atomics.get(ctx.disk_pressure, idx + 1) == 1
  end

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

  defp normalize_quorum_command({:compound_put, _redis_key, compound_key, value, expire_at_ms}),
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
    keydir
    |> Ferricstore.Store.Shard.ETS.prefix_collect_keys(prefix)
    |> Enum.each(fn compound_key ->
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

  defp offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp offheap_size(_), do: 0

  defp stored_value_size(value) when is_binary(value), do: byte_size(value)
  defp stored_value_size(value) when is_integer(value), do: byte_size(Integer.to_string(value))
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

        {:error, _reason} ->
          nil
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
          {binary(), non_neg_integer(), non_neg_integer()} | nil
  def get_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, _value, _lfu} ->
        # Hot key — value is in ETS, sendfile not applicable.
        nil

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case file_ref_from_cold_location(ctx, idx, path, offset, key, value_size, true) do
          {:ok, {file_ref_path, value_offset, size}} ->
            {file_ref_path, value_offset, size}

          nil ->
            case retry_changed_file_ref(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
              {:cold_ref, retry_path, value_offset, retry_size} ->
                {retry_path, value_offset, retry_size}

              _ ->
                nil
            end
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Invalid file ref — fall back to GenServer.
        case safe_read_call(ctx, idx, {:get_file_ref, key}) do
          {:ok, result} -> result
          :unavailable -> nil
        end

      :expired ->
        record_keyspace_miss(ctx, key)
        nil

      :miss ->
        # Key doesn't exist. No GenServer needed.
        nil

      :no_table ->
        nil
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
          | :miss
  def get_with_file_ref(ctx, key) do
    do_get_with_file_ref(ctx, key, true)
  end

  defp do_get_with_file_ref(ctx, key, validate_blob_ref?) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case file_ref_from_cold_location(
               ctx,
               idx,
               path,
               offset,
               key,
               value_size,
               validate_blob_ref?
             ) do
          {:ok, {file_ref_path, value_offset, size}} ->
            Stats.record_cold_read(ctx, key)
            {:cold_ref, file_ref_path, value_offset, size}

          nil ->
            case retry_changed_file_ref(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
              {:cold_ref, retry_path, value_offset, retry_size} ->
                Stats.record_cold_read(ctx, key)
                {:cold_ref, retry_path, value_offset, retry_size}

              {:hot, value} ->
                {:hot, value}

              :miss ->
                record_keyspace_miss(ctx, key)
                :miss
            end
        end

      {:cold, file_id, offset, value_size}
      when valid_waraft_segment_location(file_id, offset, value_size) ->
        case read_waraft_segment_materialized(ctx, idx, file_id, key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            {:cold_value, value}

          _ ->
            record_keyspace_miss(ctx, key)
            :miss
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Cold entry but no valid file ref. Ask the shard to flush pending
        # writes and return a file ref before falling back to materialization.
        shard_file_ref_or_value(ctx, idx, key)

      :expired ->
        record_keyspace_miss(ctx, key)
        :miss

      :miss ->
        if compound_data_structure_key?(ctx, keydir, key) do
          {:error, "WRONGTYPE Operation against a key holding the wrong kind of value"}
        else
          # Key not in ETS = doesn't exist. No GenServer needed.
          record_keyspace_miss(ctx, key)
          :miss
        end

      :no_table ->
        # ETS table unavailable (shard restarting). Fall back to GenServer.
        shard_file_ref_or_value(ctx, idx, key)
    end
  end

  defp shard_file_ref_or_value(ctx, idx, key) do
    case safe_read_call(ctx, idx, {:get_file_ref, key}) do
      {:ok, {path, value_offset, value_size}}
      when is_binary(path) and is_integer(value_offset) and is_integer(value_size) ->
        Stats.record_cold_read(ctx, key)
        {:cold_ref, path, value_offset, value_size}

      _ ->
        result =
          case safe_read_call(ctx, idx, {:get, key}) do
            {:ok, value} -> value
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
          {:cold_value, result}
        else
          record_keyspace_miss(ctx, key)
          :miss
        end
    end
  end

  defp compound_data_structure_key?(ctx, keydir, key) do
    case :ets.lookup(keydir, CompoundKey.type_key(key)) do
      [] -> false
      _ -> TypeRegistry.get_type(key, ctx) != "none"
    end
  rescue
    _ -> false
  end

  defp file_ref_from_cold_location(ctx, idx, path, offset, key, value_size, validate_blob_ref?) do
    if blob_ref_candidate?(ctx, value_size) do
      case cold_blob_file_ref_from_location(ctx, idx, path, offset, key, validate_blob_ref?) do
        {:ok, _file_ref} = ok ->
          ok

        :not_blob ->
          bitcask_file_ref_from_location(path, offset, key, value_size)

        {:error, _reason} ->
          nil
      end
    else
      bitcask_file_ref_from_location(path, offset, key, value_size)
    end
  end

  defp bitcask_file_ref_from_location(path, offset, key, value_size) do
    case validated_file_ref(path, offset, key, value_size) do
      {^path, value_offset, ^value_size} -> {:ok, {path, value_offset, value_size}}
      nil -> nil
    end
  end

  defp cold_blob_file_ref_from_location(ctx, idx, path, offset, key, validate_blob_ref?) do
    with {:ok, encoded_ref} <- read_cold_async(path, offset, key),
         {:ok, ref} <- BlobRef.decode(encoded_ref) do
      blob_ref_file_ref(ctx, idx, ref, validate_blob_ref?)
    else
      :error -> :not_blob
      {:error, reason} -> {:error, reason}
    end
  end

  defp blob_ref_file_ref(ctx, idx, %BlobRef{} = ref, true) do
    BlobStore.file_ref(ctx.data_dir, idx, ref)
  end

  defp blob_ref_file_ref(ctx, idx, %BlobRef{} = ref, false) do
    if BlobRef.valid?(ref),
      do: unchecked_blob_ref_file_ref(ctx, idx, ref),
      else: {:error, :invalid_blob_ref}
  end

  defp unchecked_blob_ref_file_ref(ctx, idx, %BlobRef{version: 1, size: size} = ref) do
    path = BlobRef.path(ctx.data_dir, idx, ref)

    case File.stat(path) do
      {:ok, %{type: :regular, size: ^size}} -> {:ok, {path, 0, size}}
      {:ok, %{type: :regular}} -> {:error, :size_mismatch}
      {:ok, _other} -> {:error, :invalid_blob_file}
      {:error, reason} -> {:error, reason}
    end
  end

  defp unchecked_blob_ref_file_ref(
         ctx,
         idx,
         %BlobRef{version: 2, size: size, offset: offset} = ref
       ) do
    path = BlobRef.path(ctx.data_dir, idx, ref)

    case File.stat(path) do
      {:ok, %{type: :regular, size: file_size}} when file_size >= offset + size ->
        {:ok, {path, offset, size}}

      {:ok, %{type: :regular}} ->
        {:error, :size_mismatch}

      {:ok, _other} ->
        {:error, :invalid_blob_file}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validated_file_ref(path, record_offset, key, value_size) do
    case Ferricstore.Bitcask.NIF.v2_validate_value_ref(path, record_offset, key, value_size) do
      {:ok, {value_offset, ^value_size}} ->
        {path, value_offset, value_size}

      _ ->
        maybe_run_validate_file_ref_miss_hook()
        nil
    end
  end

  defp retry_changed_file_ref(ctx, idx, keydir, key, original_location, now) do
    case retry_changed_file_ref_once(ctx, idx, keydir, key, original_location, now) do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(
          fn ->
            retry_changed_file_ref_once(ctx, idx, keydir, key, original_location, now)
          end,
          cold_retry_metadata(ctx, idx, key, :file_ref)
        )

      result ->
        result
    end
  end

  defp retry_changed_file_ref_once(ctx, idx, keydir, key, original_location, now) do
    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} != original_location ->
        path = cold_file_path(ctx, idx, file_id)

        case file_ref_from_cold_location(ctx, idx, path, offset, key, value_size, true) do
          {:ok, {file_ref_path, value_offset, size}} ->
            {:cold_ref, file_ref_path, value_offset, size}

          nil ->
            :miss
        end

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} == original_location ->
        :unchanged_cold

      _ ->
        :miss
    end
  end

  defp cold_file_path(ctx, idx, {:flow_history, file_id}) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(idx)
    |> Ferricstore.Flow.HistoryProjector.history_file_path(file_id)
  end

  defp cold_file_path(ctx, idx, file_id) do
    shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)
    Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  defp maybe_run_validate_file_ref_miss_hook do
    case Process.get(:ferricstore_router_validate_file_ref_miss_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  # Like ets_get but returns file ref info for cold entries and LFU counter for hits.
  # Single lookup provides everything needed — no second ETS read for bookkeeping.
  defp ets_get_full(ctx, idx, keydir, key, now) do
    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
          {:hit, value, lfu}

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize}

        [{^key, nil, 0, _lfu, {:flow_history, file_id} = fid, off, vsize}]
        when is_integer(file_id) and file_id >= 0 and is_integer(off) and off >= 0 and
               is_integer(vsize) and vsize >= 0 ->
          {:cold, fid, off, vsize}

        [{^key, nil, 0, _lfu, fid, off, vsize}]
        when valid_waraft_segment_location(fid, off, vsize) ->
          {:cold, fid, off, vsize}

        [{^key, nil, 0, _lfu, :pending, off, vsize}] ->
          {:cold, :pending, off, vsize}

        [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          {:hit, value, lfu}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize}

        [{^key, nil, exp, _lfu, {:flow_history, file_id} = fid, off, vsize}]
        when exp > now and is_integer(file_id) and file_id >= 0 and is_integer(off) and
               off >= 0 and is_integer(vsize) and vsize >= 0 ->
          {:cold, fid, off, vsize}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
          {:cold, fid, off, vsize}

        [{^key, nil, exp, _lfu, :pending, off, vsize}] when exp > now ->
          {:cold, :pending, off, vsize}

        [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete_known(ctx, idx, key, value)
          :ets.delete(keydir, key)
          :expired

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :no_table
    end
  end

  defp ets_get_meta_full(ctx, idx, keydir, key, now) do
    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, exp, lfu, _fid, _off, _vsize}]
        when value != nil and (exp == 0 or exp > now) ->
          {:hit, value, exp, lfu}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when (exp == 0 or exp > now) and valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize, exp}

        [{^key, nil, exp, _lfu, {:flow_history, file_id} = fid, off, vsize}]
        when (exp == 0 or exp > now) and is_integer(file_id) and file_id >= 0 and
               is_integer(off) and off >= 0 and is_integer(vsize) and vsize >= 0 ->
          {:cold, fid, off, vsize, exp}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when (exp == 0 or exp > now) and valid_waraft_segment_location(fid, off, vsize) ->
          {:cold, fid, off, vsize, exp}

        [{^key, nil, exp, _lfu, :pending, off, vsize}] when exp == 0 or exp > now ->
          {:cold, :pending, off, vsize, exp}

        [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete_known(ctx, idx, key, value)
          :ets.delete(keydir, key)
          :expired

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :no_table
    end
  end

  @doc """
  Retrieves the value for `key`, or `nil` if the key does not exist or is
  expired.

  Hot path: reads directly from ETS (no GenServer roundtrip for cached keys).
  Falls back to a GenServer call for cache misses or when the ETS table is
  temporarily unavailable (e.g. during a shard restart).

  Each successful read is recorded as either *hot* (ETS hit) or *cold*
  (Bitcask fallback) in `Ferricstore.Stats` for the `FERRICSTORE.HOTNESS`
  command and the `INFO stats` hot/cold fields.
  """
  @spec get(FerricStore.Instance.t(), binary()) :: binary() | nil
  def get(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        value

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        # Cold key — value evicted from ETS but disk location known.
        # Read directly from Bitcask via NIF, bypassing the Shard GenServer.
        # The ETS entry has valid file_id/offset from when the write committed,
        # so pread works without flushing pending writes.
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_materialized(ctx, idx, path, offset, key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, key)
            # Warm ETS: promote back to hot if value fits in cache
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            value

          _ ->
            case retry_changed_cold_value(
                   ctx,
                   idx,
                   keydir,
                   key,
                   {file_id, offset, value_size},
                   now
                 ) do
              {:cold, value, retry_file_id, retry_offset} ->
                Stats.record_cold_read(ctx, key)

                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                value

              {:hot, value} ->
                value

              :miss ->
                record_keyspace_miss(ctx, key)
                nil
            end
        end

      {:cold, file_id, offset, value_size}
      when valid_waraft_segment_location(file_id, offset, value_size) ->
        case read_waraft_segment_materialized(ctx, idx, file_id, key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            value

          _ ->
            record_keyspace_miss(ctx, key)
            nil
        end

      {:cold, _file_id, _offset, _value_size} ->
        # Cold entry but invalid file ref — ask GenServer.
        result =
          case safe_read_call(ctx, idx, {:get, key}) do
            {:ok, value} -> value
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          record_keyspace_miss(ctx, key)
        end

        result

      :expired ->
        record_keyspace_miss(ctx, key)
        nil

      :miss ->
        # Key not in ETS at all — doesn't exist. No GenServer needed.
        record_keyspace_miss(ctx, key)
        nil

      :no_table ->
        # ETS table unavailable (shard restarting). Fall back to GenServer.
        result =
          case safe_read_call(ctx, idx, {:get, key}) do
            {:ok, value} -> value
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          record_keyspace_miss(ctx, key)
        end

        result
    end
  end

  @doc false
  @spec get_with_deferred_blob_file_ref(FerricStore.Instance.t(), binary()) ::
          {:hot, binary()}
          | {:cold_ref, binary(), non_neg_integer(), non_neg_integer()}
          | {:cold_value, binary()}
          | {:error, binary()}
          | :miss
  def get_with_deferred_blob_file_ref(ctx, key), do: do_get_with_file_ref(ctx, key, false)

  @spec batch_get(FerricStore.Instance.t(), [binary()]) :: [binary() | nil]
  def batch_get(ctx, keys) do
    now = HLC.now_ms()

    {results, {cold_entries, _cold_count, hot_hits}} =
      Enum.map_reduce(keys, {[], 0, []}, fn key, {cold_entries, cold_count, hot_hits} ->
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)

        case ets_get_full(ctx, idx, keydir, key, now) do
          {:hit, value, lfu} ->
            {{:value, value}, {cold_entries, cold_count, [{keydir, key, lfu} | hot_hits]}}

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) ->
            path = cold_file_path(ctx, idx, file_id)

            entry = {ctx, idx, keydir, key, path, file_id, offset, value_size}
            {{:cold, cold_count}, {[entry | cold_entries], cold_count + 1, hot_hits}}

          {:cold, file_id, offset, value_size}
          when valid_waraft_segment_location(file_id, offset, value_size) ->
            result =
              case read_waraft_segment_materialized(ctx, idx, file_id, key) do
                {:ok, value} when is_binary(value) ->
                  Stats.record_cold_read(ctx, key)
                  warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                  value

                _ ->
                  record_keyspace_miss(ctx, key)
                  nil
              end

            {{:value, result}, {cold_entries, cold_count, hot_hits}}

          {:cold, _file_id, _offset, _value_size} ->
            result =
              case safe_read_call(ctx, idx, {:get, key}) do
                {:ok, value} -> value
                :unavailable -> nil
              end

            if result != nil do
              Stats.record_cold_read(ctx, key)
            else
              record_keyspace_miss(ctx, key)
            end

            {{:value, result}, {cold_entries, cold_count, hot_hits}}

          :expired ->
            record_keyspace_miss(ctx, key)
            {{:value, nil}, {cold_entries, cold_count, hot_hits}}

          :miss ->
            record_keyspace_miss(ctx, key)
            {{:value, nil}, {cold_entries, cold_count, hot_hits}}

          :no_table ->
            result =
              case safe_read_call(ctx, idx, {:get, key}) do
                {:ok, value} -> value
                :unavailable -> nil
              end

            if result != nil do
              Stats.record_cold_read(ctx, key)
            else
              record_keyspace_miss(ctx, key)
            end

            {{:value, result}, {cold_entries, cold_count, hot_hits}}
        end
      end)

    sampled_read_bookkeeping_batch(ctx, Enum.reverse(hot_hits), length(hot_hits))

    cold_values =
      cold_entries
      |> Enum.reverse()
      |> read_cold_batch_async(now)
      |> List.to_tuple()

    Enum.map(results, fn
      {:value, value} -> value
      {:cold, index} -> elem(cold_values, index)
    end)
  end

  @doc false
  @spec batch_get_planned(FerricStore.Instance.t(), [tuple()]) :: [binary() | nil]
  def batch_get_planned(ctx, planned_keys) do
    keys = Enum.map(planned_keys, &planned_lookup_key/1)
    batch_get(ctx, keys)
  end

  @doc """
  Batch GET variant for TCP large-value streaming.

  It performs the same single ETS pass as `batch_get/2`, but cold entries whose
  value size is at least `min_file_ref_size` are returned as validated
  `{:file_ref, path, value_offset, size}` tuples instead of being materialized
  into BEAM binaries. Stale or invalid refs fall back to the normal batched cold
  pread path.
  """
  @spec batch_get_with_file_refs(FerricStore.Instance.t(), [binary()], non_neg_integer()) :: [
          binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}
        ]
  def batch_get_with_file_refs(ctx, keys, min_file_ref_size) do
    do_batch_get_with_file_refs(ctx, keys, min_file_ref_size, true)
  end

  defp do_batch_get_with_file_refs(ctx, keys, min_file_ref_size, validate_blob_ref?) do
    now = HLC.now_ms()

    {results, {cold_entries, _cold_count, hot_hits}} =
      Enum.map_reduce(keys, {[], 0, []}, fn key, {cold_entries, cold_count, hot_hits} ->
        idx = shard_for(ctx, key)
        keydir = resolve_keydir(ctx, idx)

        case ets_get_full(ctx, idx, keydir, key, now) do
          {:hit, value, lfu} ->
            {{:value, value}, {cold_entries, cold_count, [{keydir, key, lfu} | hot_hits]}}

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) ->
            path = cold_file_path(ctx, idx, file_id)

            maybe_file_ref_or_cold_entry(
              ctx,
              idx,
              keydir,
              key,
              path,
              file_id,
              offset,
              value_size,
              min_file_ref_size,
              cold_entries,
              cold_count,
              hot_hits,
              now
            )

          {:cold, file_id, offset, value_size}
          when valid_waraft_segment_location(file_id, offset, value_size) ->
            result =
              case read_waraft_segment_materialized(ctx, idx, file_id, key) do
                {:ok, value} when is_binary(value) ->
                  Stats.record_cold_read(ctx, key)
                  warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
                  value

                _ ->
                  record_keyspace_miss(ctx, key)
                  nil
              end

            {{:value, result}, {cold_entries, cold_count, hot_hits}}

          {:cold, _file_id, _offset, _value_size} ->
            result =
              case safe_read_call(ctx, idx, {:get, key}) do
                {:ok, value} -> value
                :unavailable -> nil
              end

            if result != nil do
              Stats.record_cold_read(ctx, key)
            else
              record_keyspace_miss(ctx, key)
            end

            {{:value, result}, {cold_entries, cold_count, hot_hits}}

          :expired ->
            record_keyspace_miss(ctx, key)
            {{:value, nil}, {cold_entries, cold_count, hot_hits}}

          :miss ->
            record_keyspace_miss(ctx, key)
            {{:value, nil}, {cold_entries, cold_count, hot_hits}}

          :no_table ->
            result =
              case safe_read_call(ctx, idx, {:get, key}) do
                {:ok, value} -> value
                :unavailable -> nil
              end

            if result != nil do
              Stats.record_cold_read(ctx, key)
            else
              record_keyspace_miss(ctx, key)
            end

            {{:value, result}, {cold_entries, cold_count, hot_hits}}
        end
      end)

    sampled_read_bookkeeping_batch(ctx, Enum.reverse(hot_hits), length(hot_hits))

    cold_values =
      cold_entries
      |> Enum.reverse()
      |> read_cold_batch_file_ref_async(now, min_file_ref_size, validate_blob_ref?)
      |> List.to_tuple()

    Enum.map(results, fn
      {:value, value} -> value
      {:file_ref, path, offset, size} -> {:file_ref, path, offset, size}
      {:cold, index} -> elem(cold_values, index)
    end)
  end

  @doc false
  @spec batch_get_with_deferred_blob_file_refs(
          FerricStore.Instance.t(),
          [binary()],
          non_neg_integer()
        ) :: [binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}]
  def batch_get_with_deferred_blob_file_refs(ctx, keys, min_file_ref_size) do
    do_batch_get_with_file_refs(ctx, keys, min_file_ref_size, false)
  end

  @doc false
  @spec batch_get_with_deferred_blob_file_refs_and_presence(
          FerricStore.Instance.t(),
          [binary()],
          non_neg_integer()
        ) ::
          {[binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}],
           boolean()}
  def batch_get_with_deferred_blob_file_refs_and_presence(ctx, keys, min_file_ref_size) do
    results = do_batch_get_with_file_refs(ctx, keys, min_file_ref_size, false)
    {results, Enum.any?(results, &file_ref_read_result?/1)}
  end

  @doc false
  @spec batch_get_with_deferred_blob_file_refs_planned_and_presence(
          FerricStore.Instance.t(),
          [tuple()],
          non_neg_integer()
        ) ::
          {[binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}],
           boolean()}
  def batch_get_with_deferred_blob_file_refs_planned_and_presence(
        ctx,
        planned_keys,
        min_file_ref_size
      ) do
    keys = Enum.map(planned_keys, &planned_lookup_key/1)
    batch_get_with_deferred_blob_file_refs_and_presence(ctx, keys, min_file_ref_size)
  end

  @doc false
  @spec batch_get_with_deferred_blob_file_refs_planned(
          FerricStore.Instance.t(),
          [tuple()],
          non_neg_integer()
        ) :: [binary() | nil | {:file_ref, binary(), non_neg_integer(), non_neg_integer()}]
  def batch_get_with_deferred_blob_file_refs_planned(ctx, planned_keys, min_file_ref_size) do
    {results, _present?} =
      batch_get_with_deferred_blob_file_refs_planned_and_presence(
        ctx,
        planned_keys,
        min_file_ref_size
      )

    results
  end

  defp planned_lookup_key({_original_key, lookup_key, _shard_index, _keydir})
       when is_binary(lookup_key),
       do: lookup_key

  defp planned_lookup_key(key) when is_binary(key), do: key

  defp file_ref_read_result?({:file_ref, _path, _offset, _size}), do: true
  defp file_ref_read_result?(_value), do: false

  defp maybe_file_ref_or_cold_entry(
         ctx,
         idx,
         keydir,
         key,
         path,
         file_id,
         offset,
         value_size,
         min_file_ref_size,
         cold_entries,
         cold_count,
         hot_hits,
         now
       )
       when value_size >= min_file_ref_size do
    if blob_ref_candidate?(ctx, value_size) do
      entry = {ctx, idx, keydir, key, path, file_id, offset, value_size}
      {{:cold, cold_count}, {[entry | cold_entries], cold_count + 1, hot_hits}}
    else
      case file_ref_from_cold_location(ctx, idx, path, offset, key, value_size, true) do
        {:ok, {file_ref_path, value_offset, size}} ->
          Stats.record_cold_read(ctx, key)
          {{:file_ref, file_ref_path, value_offset, size}, {cold_entries, cold_count, hot_hits}}

        nil ->
          case retry_changed_file_ref(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
            {:cold_ref, retry_path, value_offset, retry_size} ->
              Stats.record_cold_read(ctx, key)

              {{:file_ref, retry_path, value_offset, retry_size},
               {cold_entries, cold_count, hot_hits}}

            {:hot, value} ->
              {{:value, value}, {cold_entries, cold_count, hot_hits}}

            :miss ->
              record_keyspace_miss(ctx, key)
              {{:value, nil}, {cold_entries, cold_count, hot_hits}}
          end
      end
    end
  end

  defp maybe_file_ref_or_cold_entry(
         ctx,
         idx,
         keydir,
         key,
         path,
         file_id,
         offset,
         value_size,
         _min_file_ref_size,
         cold_entries,
         cold_count,
         hot_hits,
         _now
       ) do
    entry = {ctx, idx, keydir, key, path, file_id, offset, value_size}
    {{:cold, cold_count}, {[entry | cold_entries], cold_count + 1, hot_hits}}
  end

  defp read_cold_batch_async([], _now), do: []

  defp read_cold_batch_async(entries, now) do
    {unique_entries, value_indexes} = dedupe_cold_batch_entries(entries)
    unique_values = read_unique_cold_batch_async(unique_entries, now) |> List.to_tuple()

    Enum.map(value_indexes, fn index -> elem(unique_values, index) end)
  end

  defp read_cold_batch_file_ref_async([], _now, _min_file_ref_size, _validate_blob_ref?), do: []

  defp read_cold_batch_file_ref_async(entries, now, min_file_ref_size, validate_blob_ref?) do
    {unique_entries, value_indexes} = dedupe_cold_batch_entries(entries)

    unique_values =
      unique_entries
      |> read_unique_cold_batch_file_ref_async(now, min_file_ref_size, validate_blob_ref?)
      |> List.to_tuple()

    Enum.map(value_indexes, fn index -> elem(unique_values, index) end)
  end

  defp dedupe_cold_batch_entries(entries) do
    {unique_entries, _index_by_location, value_indexes} =
      Enum.reduce(entries, {[], %{}, []}, fn entry, {unique_acc, index_acc, value_index_acc} ->
        location = cold_batch_entry_location(entry)

        case Map.fetch(index_acc, location) do
          {:ok, index} ->
            {unique_acc, index_acc, [index | value_index_acc]}

          :error ->
            index = map_size(index_acc)
            {[entry | unique_acc], Map.put(index_acc, location, index), [index | value_index_acc]}
        end
      end)

    {Enum.reverse(unique_entries), Enum.reverse(value_indexes)}
  end

  defp cold_batch_entry_location({_ctx, _idx, _keydir, key, path, _file_id, offset, _value_size}) do
    {path, offset, key}
  end

  defp read_unique_cold_batch_async(entries, now) do
    locations =
      Enum.map(entries, fn {_ctx, _idx, _keydir, key, path, _file_id, offset, _value_size} ->
        {path, offset, key}
      end)

    values =
      case router_pread_batch_keyed(locations, @cold_batch_read_timeout_ms) do
        {:ok, values} when is_list(values) ->
          if length(values) == length(entries) do
            values
          else
            List.duplicate({:error, :batch_result_length_mismatch}, length(entries))
          end

        {:error, reason} ->
          List.duplicate({:error, reason}, length(entries))
      end

    entry_values = Enum.zip(entries, values)

    corrupt_by_path =
      Enum.reduce(entry_values, %{}, fn
        {{_ctx, _idx, _keydir, _key, _path, _file_id, _offset, _value_size}, value},
        corrupt_by_path
        when is_binary(value) ->
          corrupt_by_path

        {{_ctx, _idx, _keydir, _key, path, _file_id, _offset, _value_size}, value},
        corrupt_by_path ->
          reason = cold_batch_read_error_reason(value)
          Map.update(corrupt_by_path, {path, reason}, 1, &(&1 + 1))
      end)

    emit_batch_cold_read_corruption(corrupt_by_path)

    entry_values = materialize_cold_batch_values(entry_values)

    Enum.map(entry_values, fn
      {{ctx, idx, keydir, key, _path, file_id, offset, _value_size}, {:ok, value}}
      when is_binary(value) ->
        Stats.record_cold_read(ctx, key)
        warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
        value

      {{ctx, idx, keydir, key, _path, file_id, offset, value_size}, _value_or_error} ->
        case retry_changed_cold_value(ctx, idx, keydir, key, {file_id, offset, value_size}, now) do
          {:cold, value, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, retry_file_id, retry_offset)
            value

          {:hot, value} ->
            value

          :miss ->
            record_keyspace_miss(ctx, key)
            nil
        end
    end)
  end

  defp materialize_cold_batch_values(entry_values) do
    {groups, results} =
      entry_values
      |> Enum.with_index()
      |> Enum.reduce({%{}, %{}}, fn
        {{{ctx, idx, _keydir, _key, _path, _file_id, _offset, _value_size}, value}, index},
        {groups, results}
        when is_binary(value) ->
          group_key = {ctx.data_dir, idx, BlobValue.threshold(ctx)}
          groups = Map.update(groups, group_key, [{index, value}], &[{index, value} | &1])
          {groups, results}

        {{_entry, value_or_error}, index}, {groups, results} ->
          {groups, Map.put(results, index, value_or_error)}
      end)

    results =
      Enum.reduce(groups, results, fn {{data_dir, idx, threshold}, indexed_values}, acc ->
        indexed_values = Enum.reverse(indexed_values)
        {indexes, values} = Enum.unzip(indexed_values)
        materialized = BlobValue.maybe_materialize_many(data_dir, idx, threshold, values)

        indexes
        |> Enum.zip(materialized)
        |> Enum.reduce(acc, fn {index, result}, acc -> Map.put(acc, index, result) end)
      end)

    entry_values
    |> Enum.with_index()
    |> Enum.map(fn {{entry, _value}, index} ->
      {entry, Map.fetch!(results, index)}
    end)
  end

  defp read_unique_cold_batch_file_ref_async(
         entries,
         now,
         min_file_ref_size,
         validate_blob_ref?
       ) do
    locations =
      Enum.map(entries, fn {_ctx, _idx, _keydir, key, path, _file_id, offset, _value_size} ->
        {path, offset, key}
      end)

    values =
      case router_pread_batch_keyed(locations, @cold_batch_read_timeout_ms) do
        {:ok, values} when is_list(values) ->
          if length(values) == length(entries) do
            values
          else
            List.duplicate({:error, :batch_result_length_mismatch}, length(entries))
          end

        {:error, reason} ->
          List.duplicate({:error, reason}, length(entries))
      end

    entry_values = Enum.zip(entries, values)

    corrupt_by_path =
      Enum.reduce(entry_values, %{}, fn
        {{_ctx, _idx, _keydir, _key, _path, _file_id, _offset, _value_size}, value},
        corrupt_by_path
        when is_binary(value) ->
          corrupt_by_path

        {{_ctx, _idx, _keydir, _key, path, _file_id, _offset, _value_size}, value},
        corrupt_by_path ->
          reason = cold_batch_read_error_reason(value)
          Map.update(corrupt_by_path, {path, reason}, 1, &(&1 + 1))
      end)

    emit_batch_cold_read_corruption(corrupt_by_path)

    blob_file_ref_results =
      batch_blob_file_ref_results(entry_values, min_file_ref_size, validate_blob_ref?)

    entry_values
    |> Enum.with_index()
    |> Enum.map(fn
      {{entry, _value}, index} when is_map_key(blob_file_ref_results, index) ->
        cold_batch_preloaded_blob_file_ref_value(
          entry,
          Map.fetch!(blob_file_ref_results, index),
          now
        )

      {{entry, value}, _index} when is_binary(value) ->
        cold_batch_file_ref_value(entry, value, min_file_ref_size, validate_blob_ref?, now)

      {{{ctx, idx, keydir, key, _path, file_id, offset, value_size}, _value}, _index} ->
        retry_cold_batch_materialized_value(
          ctx,
          idx,
          keydir,
          key,
          {file_id, offset, value_size},
          now
        )
    end)
  end

  defp batch_blob_file_ref_results(entry_values, min_file_ref_size, validate_blob_ref?) do
    grouped =
      entry_values
      |> Enum.with_index()
      |> Enum.reduce(%{}, fn
        {{{ctx, idx, _keydir, _key, _path, _file_id, _offset, value_size}, value}, index}, acc
        when is_binary(value) ->
          with true <- blob_ref_candidate?(ctx, value_size),
               {:ok, %BlobRef{size: blob_size} = ref} when blob_size >= min_file_ref_size <-
                 BlobRef.decode(value) do
            Map.update(acc, {ctx.data_dir, idx}, [{index, ref}], &[{index, ref} | &1])
          else
            _ -> acc
          end

        _other, acc ->
          acc
      end)

    Enum.reduce(grouped, %{}, fn {{data_dir, idx}, indexed_refs}, acc ->
      indexed_refs = Enum.reverse(indexed_refs)
      {indexes, refs} = Enum.unzip(indexed_refs)

      results =
        if validate_blob_ref? do
          BlobStore.file_refs_many(data_dir, idx, refs)
        else
          Enum.map(refs, &blob_ref_file_ref(%{data_dir: data_dir}, idx, &1, false))
        end

      indexes
      |> Enum.zip(results)
      |> Enum.reduce(acc, fn {index, result}, acc -> Map.put(acc, index, result) end)
    end)
  end

  defp cold_batch_preloaded_blob_file_ref_value(
         {ctx, _idx, _keydir, key, _path, _file_id, _offset, _value_size},
         {:ok, {path, value_offset, size}},
         _now
       ) do
    Stats.record_cold_read(ctx, key)
    {:file_ref, path, value_offset, size}
  end

  defp cold_batch_preloaded_blob_file_ref_value(
         {ctx, idx, keydir, key, _path, file_id, offset, value_size},
         {:error, _reason},
         now
       ) do
    retry_cold_batch_materialized_value(
      ctx,
      idx,
      keydir,
      key,
      {file_id, offset, value_size},
      now
    )
  end

  defp cold_batch_file_ref_value(
         {ctx, idx, keydir, key, _path, file_id, offset, value_size},
         value,
         min_file_ref_size,
         validate_blob_ref?,
         now
       ) do
    if blob_ref_candidate?(ctx, value_size) do
      case BlobRef.decode(value) do
        {:ok, %BlobRef{size: blob_size} = ref} when blob_size >= min_file_ref_size ->
          case blob_ref_file_ref(ctx, idx, ref, validate_blob_ref?) do
            {:ok, {path, value_offset, size}} ->
              Stats.record_cold_read(ctx, key)
              {:file_ref, path, value_offset, size}

            {:error, _reason} ->
              retry_cold_batch_materialized_value(
                ctx,
                idx,
                keydir,
                key,
                {file_id, offset, value_size},
                now
              )
          end

        {:ok, %BlobRef{} = ref} ->
          case BlobStore.get(ctx.data_dir, idx, ref) do
            {:ok, materialized} ->
              Stats.record_cold_read(ctx, key)
              warm_ets_after_cold_read(ctx, idx, keydir, key, materialized, file_id, offset)
              materialized

            {:error, _reason} ->
              retry_cold_batch_materialized_value(
                ctx,
                idx,
                keydir,
                key,
                {file_id, offset, value_size},
                now
              )
          end

        :error ->
          Stats.record_cold_read(ctx, key)
          warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
          value
      end
    else
      Stats.record_cold_read(ctx, key)
      warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
      value
    end
  end

  defp retry_cold_batch_materialized_value(ctx, idx, keydir, key, original_location, now) do
    case retry_changed_cold_value(ctx, idx, keydir, key, original_location, now) do
      {:cold, value, retry_file_id, retry_offset} ->
        Stats.record_cold_read(ctx, key)
        warm_ets_after_cold_read(ctx, idx, keydir, key, value, retry_file_id, retry_offset)
        value

      {:hot, value} ->
        value

      :miss ->
        record_keyspace_miss(ctx, key)
        nil
    end
  end

  defp router_pread_batch_keyed(locations, timeout_ms) do
    case Process.get(:ferricstore_router_pread_batch_keyed_result) do
      nil -> Ferricstore.Store.ColdRead.pread_batch_keyed(locations, timeout_ms)
      forced_result -> forced_result
    end
  end

  defp emit_batch_cold_read_corruption(corrupt_by_path) when map_size(corrupt_by_path) == 0,
    do: :ok

  defp emit_batch_cold_read_corruption(corrupt_by_path) do
    Enum.each(corrupt_by_path, fn {{path, reason}, count} ->
      :telemetry.execute(
        [:ferricstore, :bitcask, :pread_corrupt],
        %{count: count},
        %{path: path, reason: reason}
      )
    end)
  end

  defp cold_batch_read_error_reason({:error, reason}) when is_binary(reason) do
    downcased = String.downcase(reason)

    if String.contains?(downcased, "missing_file") or
         String.contains?(downcased, "no such file") do
      :missing_file
    else
      :corrupt_record
    end
  end

  defp cold_batch_read_error_reason({:error, reason}) when reason in [:missing_file, :enoent],
    do: :missing_file

  defp cold_batch_read_error_reason({:error, :timeout}), do: :timeout

  defp cold_batch_read_error_reason({:error, :batch_result_length_mismatch}),
    do: :batch_result_length_mismatch

  defp cold_batch_read_error_reason({:error, _reason}), do: :corrupt_record

  defp cold_batch_read_error_reason(_value), do: :nil_from_cold_location

  defp read_cold_async(path, offset, expected_key) do
    Ferricstore.Store.ColdRead.pread_at(path, offset, expected_key, @cold_batch_read_timeout_ms)
  end

  defp read_cold_materialized(ctx, idx, path, offset, expected_key) do
    with {:ok, value} <- read_cold_async(path, offset, expected_key),
         {:ok, materialized} <- materialize_blob_value(ctx, idx, value) do
      {:ok, materialized}
    end
  end

  defp read_waraft_segment_materialized(ctx, idx, file_id, key) do
    with {:ok, value} <-
           Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(ctx, idx, file_id, key),
         {:ok, materialized} <- materialize_blob_value(ctx, idx, value) do
      {:ok, materialized}
    end
  end

  defp materialize_blob_value(ctx, idx, value) do
    BlobValue.maybe_materialize(ctx.data_dir, idx, BlobValue.threshold(ctx), value)
  end

  defp blob_ref_candidate?(ctx, value_size) do
    BlobValue.threshold(ctx) > 0 and BlobRef.encoded_size?(value_size)
  end

  defp retry_changed_cold_value(ctx, idx, keydir, key, original_location, now) do
    case retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now) do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(
          fn ->
            retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now)
          end,
          cold_retry_metadata(ctx, idx, key, :value)
        )

      result ->
        result
    end
  end

  defp retry_changed_cold_value_once(ctx, idx, keydir, key, original_location, now) do
    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value}

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} != original_location ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_materialized(ctx, idx, path, offset, key) do
          {:ok, value} when is_binary(value) -> {:cold, value, file_id, offset}
          _ -> :miss
        end

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} == original_location ->
        :unchanged_cold

      _ ->
        :miss
    end
  end

  defp retry_changed_cold_meta(ctx, idx, keydir, key, original_location, now) do
    case retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now) do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(
          fn ->
            retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now)
          end,
          cold_retry_metadata(ctx, idx, key, :meta)
        )

      result ->
        result
    end
  end

  defp retry_changed_cold_meta_once(ctx, idx, keydir, key, original_location, now) do
    case ets_get_meta_full(ctx, idx, keydir, key, now) do
      {:hit, value, expire_at_ms, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {:hot, value, expire_at_ms}

      {:cold, file_id, offset, value_size, expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} != original_location ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_materialized(ctx, idx, path, offset, key) do
          {:ok, value} when is_binary(value) -> {:cold, value, expire_at_ms, file_id, offset}
          _ -> :miss
        end

      {:cold, file_id, offset, value_size, _expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) and
             {file_id, offset, value_size} == original_location ->
        :unchanged_cold

      _ ->
        :miss
    end
  end

  defp retry_after_unchanged_cold_location(retry_fun, metadata) when is_function(retry_fun, 0) do
    retry_after_unchanged_cold_location(retry_fun, metadata, @cold_location_retry_attempts)
  end

  defp retry_after_unchanged_cold_location(_retry_fun, metadata, 0) do
    emit_cold_retry_exhausted(metadata)
    :miss
  end

  defp retry_after_unchanged_cold_location(retry_fun, metadata, attempts_left) do
    maybe_run_cold_location_miss_hook()
    Process.sleep(@cold_location_retry_sleep_ms)

    case retry_fun.() do
      :unchanged_cold ->
        retry_after_unchanged_cold_location(retry_fun, metadata, attempts_left - 1)

      result ->
        result
    end
  end

  defp cold_retry_metadata(ctx, idx, key, operation) do
    %{
      instance: ctx.name,
      shard_index: idx,
      operation: operation,
      reason: :unchanged_cold_location,
      redis_key_hash: :erlang.phash2(key)
    }
  end

  defp emit_cold_retry_exhausted(nil), do: :ok

  defp emit_cold_retry_exhausted(metadata) do
    :telemetry.execute(
      [:ferricstore, :store, :cold_read_retry_exhausted],
      %{count: 1, attempts: @cold_location_retry_attempts},
      metadata
    )
  end

  defp maybe_run_cold_location_miss_hook do
    case Process.get(:ferricstore_router_cold_location_miss_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  @doc """
  Returns `{value, expire_at_ms}` for a live key, or `nil` if the key does
  not exist or is expired.

  Hot path: reads directly from ETS for cached keys. Each read is recorded
  as hot or cold in `Ferricstore.Stats`.
  """
  @spec get_meta(FerricStore.Instance.t(), binary()) :: {binary(), non_neg_integer()} | nil
  def get_meta(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_meta_full(ctx, idx, keydir, key, now) do
      {:hit, value, expire_at_ms, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        {value, expire_at_ms}

      {:cold, file_id, offset, value_size, expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) ->
        # Cold key — read value from disk directly, return with expire_at_ms.
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_materialized(ctx, idx, path, offset, key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            {value, expire_at_ms}

          _ ->
            case retry_changed_cold_meta(
                   ctx,
                   idx,
                   keydir,
                   key,
                   {file_id, offset, value_size},
                   now
                 ) do
              {:cold, value, retry_expire_at_ms, retry_file_id, retry_offset} ->
                Stats.record_cold_read(ctx, key)

                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                {value, retry_expire_at_ms}

              {:hot, value, retry_expire_at_ms} ->
                {value, retry_expire_at_ms}

              :miss ->
                record_keyspace_miss(ctx, key)
                nil
            end
        end

      {:cold, file_id, offset, value_size, expire_at_ms}
      when valid_waraft_segment_location(file_id, offset, value_size) ->
        case read_waraft_segment_materialized(ctx, idx, file_id, key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            {value, expire_at_ms}

          _ ->
            record_keyspace_miss(ctx, key)
            nil
        end

      {:cold, _file_id, _offset, _value_size, _expire_at_ms} ->
        # Invalid file ref — ask GenServer.
        result =
          case safe_read_call(ctx, idx, {:get_meta, key}) do
            {:ok, result} -> result
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          record_keyspace_miss(ctx, key)
        end

        result

      :expired ->
        record_keyspace_miss(ctx, key)
        nil

      :miss ->
        record_keyspace_miss(ctx, key)
        nil

      :no_table ->
        result =
          case safe_read_call(ctx, idx, {:get_meta, key}) do
            {:ok, result} -> result
            :unavailable -> nil
          end

        if result != nil do
          Stats.record_cold_read(ctx, key)
        else
          record_keyspace_miss(ctx, key)
        end

        result
    end
  end

  @doc """
  Returns the expiry timestamp for a live plain key without reading its value.

  This is used by expiry-time commands so cold large values do not pay a
  Bitcask pread just to report TTL metadata.
  """
  @spec expire_at_ms(FerricStore.Instance.t(), binary()) :: non_neg_integer() | nil
  def expire_at_ms(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          0

        [{^key, nil, 0, _lfu, fid, off, vsize}]
        when valid_cold_location(fid, off, vsize) ->
          0

        [{^key, nil, 0, _lfu, fid, off, vsize}]
        when valid_waraft_segment_location(fid, off, vsize) ->
          0

        [{^key, nil, 0, _lfu, :pending, _off, vsize}]
        when valid_pending_value_size(vsize) ->
          0

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          exp

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          exp

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
          exp

        [{^key, nil, exp, _lfu, :pending, _off, vsize}]
        when exp > now and valid_pending_value_size(vsize) ->
          exp

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          nil

        [] ->
          nil
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :expire_at_ms, nil)
    end
  end

  @doc """
  Returns the live plain key value size without reading a cold value.

  Hot entries use the in-memory value size; cold entries use the keydir
  `value_size` field populated by Bitcask append/recovery.
  """
  @spec value_size(FerricStore.Instance.t(), binary()) :: non_neg_integer() | nil
  def value_size(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, _lfu, _fid, _off, _vsize}] when value != nil ->
          stored_value_size(value)

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          cold_logical_value_size(ctx, idx, key, fid, off, vsize)

        [{^key, nil, 0, _lfu, fid, off, vsize}]
        when valid_waraft_segment_location(fid, off, vsize) ->
          vsize

        [{^key, nil, 0, _lfu, :pending, _off, vsize}]
        when valid_pending_value_size(vsize) ->
          vsize

        [{^key, value, exp, _lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          stored_value_size(value)

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          cold_logical_value_size(ctx, idx, key, fid, off, vsize)

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
          vsize

        [{^key, nil, exp, _lfu, :pending, _off, vsize}]
        when exp > now and valid_pending_value_size(vsize) ->
          vsize

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          nil

        [] ->
          nil
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :value_size, nil)
    end
  end

  @doc false
  @spec object_lfu(FerricStore.Instance.t(), binary()) :: non_neg_integer() | nil
  def object_lfu(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
          lfu

        [{^key, nil, 0, lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          lfu

        [{^key, nil, 0, lfu, fid, off, vsize}]
        when valid_waraft_segment_location(fid, off, vsize) ->
          lfu

        [{^key, nil, 0, lfu, :pending, _off, vsize}]
        when valid_pending_value_size(vsize) ->
          lfu

        [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
          lfu

        [{^key, nil, exp, lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          lfu

        [{^key, nil, exp, lfu, fid, off, vsize}]
        when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
          lfu

        [{^key, nil, exp, lfu, :pending, _off, vsize}]
        when exp > now and valid_pending_value_size(vsize) ->
          lfu

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          nil

        [] ->
          nil
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :object_lfu, nil)
    end
  end

  @doc """
  Returns a byte range for a live plain key without reading the full cold value.

  Hot entries slice the in-memory value. Cold entries validate the Bitcask
  location once, then read only the requested value bytes from the data file.
  Missing or expired keys return `nil`, matching `get/2`.
  """
  @spec getrange(FerricStore.Instance.t(), binary(), integer(), integer()) :: binary() | nil
  def getrange(ctx, key, start_idx, end_idx) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, key, lfu)
        range_from_value(value, start_idx, end_idx)

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        cold_range_from_location(
          ctx,
          idx,
          keydir,
          key,
          file_id,
          offset,
          value_size,
          start_idx,
          end_idx,
          now
        )

      {:cold, file_id, offset, value_size}
      when valid_waraft_segment_location(file_id, offset, value_size) ->
        case read_waraft_segment_materialized(ctx, idx, file_id, key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, key)
            warm_ets_after_cold_read(ctx, idx, keydir, key, value, file_id, offset)
            range_from_value(value, start_idx, end_idx)

          _ ->
            record_keyspace_miss(ctx, key)
            nil
        end

      {:cold, _file_id, _offset, _value_size} ->
        fallback_getrange(ctx, idx, key, start_idx, end_idx)

      :expired ->
        record_keyspace_miss(ctx, key)
        nil

      :miss ->
        record_keyspace_miss(ctx, key)
        nil

      :no_table ->
        fallback_getrange(ctx, idx, key, start_idx, end_idx)
    end
  end

  defp cold_range_from_location(
         ctx,
         idx,
         keydir,
         key,
         file_id,
         offset,
         value_size,
         start_idx,
         end_idx,
         now
       ) do
    if blob_ref_candidate?(ctx, value_size) do
      path = cold_file_path(ctx, idx, file_id)

      case cold_blob_range_from_location(ctx, idx, path, offset, key, start_idx, end_idx) do
        {:ok, value} ->
          Stats.record_cold_read(ctx, key)
          value

        :not_blob ->
          cold_bitcask_range_from_location(
            ctx,
            idx,
            keydir,
            key,
            file_id,
            offset,
            value_size,
            start_idx,
            end_idx,
            now
          )

        {:error, _reason} ->
          retry_getrange_after_ref_miss(
            ctx,
            idx,
            keydir,
            key,
            {file_id, offset, value_size},
            start_idx,
            end_idx,
            now
          )
      end
    else
      cold_bitcask_range_from_location(
        ctx,
        idx,
        keydir,
        key,
        file_id,
        offset,
        value_size,
        start_idx,
        end_idx,
        now
      )
    end
  end

  defp cold_bitcask_range_from_location(
         ctx,
         idx,
         keydir,
         key,
         file_id,
         offset,
         value_size,
         start_idx,
         end_idx,
         now
       ) do
    case normalize_byte_range(value_size, start_idx, end_idx) do
      :empty ->
        ""

      {relative_offset, count} ->
        path = cold_file_path(ctx, idx, file_id)

        case validated_file_ref(path, offset, key, value_size) do
          {^path, value_offset, ^value_size} ->
            case read_validated_value_range(ctx, key, path, value_offset + relative_offset, count) do
              {:ok, value} ->
                value

              :error ->
                retry_getrange_after_ref_miss(
                  ctx,
                  idx,
                  keydir,
                  key,
                  {file_id, offset, value_size},
                  start_idx,
                  end_idx,
                  now
                )
            end

          nil ->
            retry_getrange_after_ref_miss(
              ctx,
              idx,
              keydir,
              key,
              {file_id, offset, value_size},
              start_idx,
              end_idx,
              now
            )
        end
    end
  end

  defp cold_blob_range_from_location(ctx, idx, path, offset, key, start_idx, end_idx) do
    with {:ok, encoded_ref} <- read_cold_async(path, offset, key),
         {:ok, %BlobRef{} = ref} <- BlobRef.decode(encoded_ref) do
      case normalize_byte_range(ref.size, start_idx, end_idx) do
        :empty ->
          {:ok, ""}

        {relative_offset, count} ->
          BlobStore.get_range(ctx.data_dir, idx, ref, relative_offset, count)
      end
    else
      :error -> :not_blob
      {:error, reason} -> {:error, reason}
    end
  end

  defp retry_getrange_after_ref_miss(
         ctx,
         idx,
         keydir,
         key,
         original_location,
         start_idx,
         end_idx,
         now
       ) do
    case retry_changed_file_ref(ctx, idx, keydir, key, original_location, now) do
      {:cold_ref, path, value_offset, value_size} ->
        case normalize_byte_range(value_size, start_idx, end_idx) do
          :empty ->
            ""

          {relative_offset, count} ->
            case read_validated_value_range(ctx, key, path, value_offset + relative_offset, count) do
              {:ok, value} ->
                value

              :error ->
                record_keyspace_miss(ctx, key)
                nil
            end
        end

      {:hot, value} ->
        range_from_value(value, start_idx, end_idx)

      :miss ->
        record_keyspace_miss(ctx, key)
        nil
    end
  end

  defp read_validated_value_range(ctx, key, path, offset, count) do
    maybe_run_cold_range_pread_miss_hook()

    case pread_file_range(path, offset, count) do
      {:ok, value} ->
        Stats.record_cold_read(ctx, key)
        {:ok, value}

      :error ->
        :error
    end
  end

  defp maybe_run_cold_range_pread_miss_hook do
    case Process.get(:ferricstore_router_cold_range_pread_miss_hook) do
      fun when is_function(fun, 0) -> fun.()
      _ -> :ok
    end
  end

  defp fallback_getrange(ctx, idx, key, start_idx, end_idx) do
    result =
      case safe_read_call(ctx, idx, {:get, key}) do
        {:ok, value} -> value
        :unavailable -> nil
      end

    if result != nil do
      Stats.record_cold_read(ctx, key)
      range_from_value(result, start_idx, end_idx)
    else
      record_keyspace_miss(ctx, key)
      nil
    end
  end

  defp pread_file_range(_path, _offset, 0), do: {:ok, ""}

  defp pread_file_range(path, offset, count) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case :file.pread(fd, offset, count) do
            {:ok, value} when is_binary(value) and byte_size(value) == count -> {:ok, value}
            _ -> :error
          end
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :error
    end
  end

  defp range_from_value(value, start_idx, end_idx) when is_binary(value),
    do: slice_binary_range(value, start_idx, end_idx)

  defp range_from_value(value, start_idx, end_idx) when is_integer(value),
    do: value |> Integer.to_string() |> slice_binary_range(start_idx, end_idx)

  defp range_from_value(value, start_idx, end_idx) when is_float(value),
    do: value |> Float.to_string() |> slice_binary_range(start_idx, end_idx)

  defp range_from_value(value, start_idx, end_idx),
    do: value |> to_string() |> slice_binary_range(start_idx, end_idx)

  defp slice_binary_range(value, start_idx, end_idx) do
    case normalize_byte_range(byte_size(value), start_idx, end_idx) do
      :empty -> ""
      {offset, count} -> binary_part(value, offset, count)
    end
  end

  defp normalize_byte_range(0, _start_idx, _end_idx), do: :empty

  defp normalize_byte_range(size, start_idx, end_idx) when size > 0 do
    start_norm = if start_idx < 0, do: max(size + start_idx, 0), else: start_idx
    end_norm = if end_idx < 0, do: size + end_idx, else: end_idx

    start_clamped = min(start_norm, size)
    end_clamped = min(end_norm, size - 1)

    if start_clamped > end_clamped do
      :empty
    else
      {start_clamped, end_clamped - start_clamped + 1}
    end
  end

  # Sampling rate for read-side bookkeeping (LFU touch + hot/cold stats).
  # 1 in N reads performs the ETS writes. Reduces write contention at high
  # concurrency with negligible impact on LFU accuracy (logarithmic counter)
  # and stats precision (ratio stays the same).
  # Default 100 = sample 1 in 100 reads. Set to 1 to disable sampling.

  # LFU counter already available from the initial ets_get_full lookup.
  # Eliminates the second ETS lookup that sampled_read_bookkeeping does.
  defp sampled_read_bookkeeping_fast(ctx, keydir, key, lfu) do
    sampled = Stats.sample_keyspace_hits_for_key(ctx, key)

    if sampled > 0 do
      LFU.touch(ctx, keydir, key, lfu)
      Stats.record_hot_read(ctx, key)
    end
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
        replaced =
          :ets.select_replace(keydir, [
            {
              {key, nil, :"$1", :"$2", file_id, offset, :"$3"},
              [],
              [{{key, value, :"$1", lfu, file_id, offset, :"$3"}}]
            }
          ])

        if replaced > 0 do
          track_keydir_binary_warm(ctx, idx, value)
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
    if ctx.name == :default and PerfToggles.direct_batch_commands?() do
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
    if PerfToggles.direct_batch_commands?() do
      do_batch_quorum_put_entries(ctx, kv_pairs, origin_node)
    else
      batch_quorum_commands(ctx, put_entries_to_keyed_commands(kv_pairs), origin_node)
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
    if PerfToggles.direct_batch_commands?() do
      do_batch_quorum_delete_keys(ctx, keys, origin_node)
    else
      batch_quorum_commands(ctx, delete_keys_to_keyed_commands(keys), origin_node)
    end
  end

  defp put_entries_to_keyed_commands(entries) do
    Enum.map(entries, fn entry ->
      {key, value, expire_at_ms} = normalize_put_batch_entry(entry)
      {key, {:put, key, value, expire_at_ms}}
    end)
  end

  defp delete_keys_to_keyed_commands(keys) do
    Enum.map(keys, fn key -> {key, {:delete, key}} end)
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
            0..(ctx.shard_count - 1)
            |> Enum.reduce_while(:ok, fn idx, :ok ->
              case raft_write(ctx, idx, key, {:put, key, value, expire_at_ms}) do
                :ok -> {:cont, :ok}
                {:error, _reason} = error -> {:halt, error}
                other -> {:halt, other}
              end
            end)

          {:error, _} = err ->
            err
        end
    end
  end

  @doc false
  def flow_get(ctx, id, partition_key) when is_binary(id) do
    key = Ferricstore.Flow.Keys.state_key(id, partition_key)

    case Stats.with_cache_tracking_disabled(fn -> get(ctx, key) end) do
      nil -> flow_get_lmdb(ctx, key, :lagged)
      value -> value
    end
  end

  @doc false
  def flow_batch_get(ctx, ids, partition_key) when is_list(ids) do
    keys = Enum.map(ids, &Ferricstore.Flow.Keys.state_key(&1, partition_key))

    values =
      Stats.with_cache_tracking_disabled(fn ->
        batch_get(ctx, keys)
      end)

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
        values = flow_lmdb_get_many(path, group_keys, now_ms, mode)

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

    path
    |> Ferricstore.Flow.LMDB.get(key)
    |> flow_decode_lmdb_get_result(path, key, CommandTime.now_ms(), mode)
  end

  defp flow_lmdb_path(ctx, key) do
    idx = shard_for(ctx, key)
    ctx.data_dir |> Ferricstore.DataDir.shard_data_path(idx) |> Ferricstore.Flow.LMDB.path()
  end

  defp flow_lmdb_get_many(_path, [], _now_ms, _mode), do: []

  defp flow_lmdb_get_many(path, keys, now_ms, mode) do
    case Ferricstore.Flow.LMDB.get_many(path, keys) do
      {:ok, results} ->
        keys
        |> Enum.zip(results)
        |> Enum.map(fn {key, result} ->
          flow_decode_lmdb_get_result(result, path, key, now_ms, mode)
        end)

      {:error, reason} ->
        flow_observe_lmdb_read_error(mode, reason)

        Enum.map(keys, fn key ->
          path
          |> Ferricstore.Flow.LMDB.get(key)
          |> flow_decode_lmdb_get_result(path, key, now_ms, mode)
        end)
    end
  end

  defp flow_decode_lmdb_get_result({:ok, blob}, path, key, now_ms, mode) when is_binary(blob) do
    case Ferricstore.Flow.LMDB.decode_value(blob, now_ms) do
      {:ok, value} ->
        value

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

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
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

            if byte_size(key) > @max_key_size do
              {valid_acc,
               Map.put(
                 result_acc,
                 idx,
                 {:error, "ERR key too large (max #{@max_key_size} bytes)"}
               )}
            else
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

    if byte_size(key) > @max_key_size do
      {:error, "ERR key too large (max #{@max_key_size} bytes)"}
    else
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
    flow_many_by_shard(ctx, attrs_list, :flow_create_many, "__batch__")
  end

  @doc false
  def flow_create_many_independent(_ctx, []), do: []

  def flow_create_many_independent(ctx, attrs_list) when is_list(attrs_list) do
    flow_create_pipeline_batch(ctx, attrs_list)
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

    # Flow child closure can discover additional child shards while applying
    # parent terminal policies, so take a conservative all-shard transaction.
    shards = Enum.to_list(0..(ctx.shard_count - 1))

    anchor_idx = hd(shards)

    shard_batches =
      Enum.map(shards, fn
        ^anchor_idx -> {anchor_idx, [{0, entry}], nil}
        shard_idx -> {shard_idx, [], nil}
      end)

    result =
      raft_write(ctx, anchor_idx, "f:{flow-cross-shard}:tx", {:cross_shard_tx, shard_batches})

    case result do
      %{^anchor_idx => [reply]} -> reply
      {:error, _reason} = error -> error
      other -> other
    end
  end

  defp flow_many_by_shard(ctx, attrs_list, command, batch_id)
       when command in [
              :flow_create_many,
              :flow_complete_many,
              :flow_cancel_many,
              :flow_fail_many,
              :flow_retry_many,
              :flow_transition_many
            ] do
    {buckets, count} =
      Enum.reduce(attrs_list, {flow_fixed_shard_buckets(ctx.shard_count), 0}, fn
        %{id: id, partition_key: partition_key} = attrs, {buckets, idx}
        when is_binary(id) and is_binary(partition_key) ->
          key = Ferricstore.Flow.Keys.state_key(id, partition_key)
          shard_idx = shard_for(ctx, key)

          {flow_put_shard_bucket(buckets, shard_idx, {idx, attrs}), idx + 1}
      end)

    groups =
      flow_nonempty_shard_buckets(buckets, ctx.shard_count, fn shard_idx, entries ->
        group = Enum.reverse(entries)
        attrs = Enum.map(group, fn {_idx, attrs} -> attrs end)
        partition_key = attrs |> hd() |> Map.fetch!(:partition_key)
        key = Ferricstore.Flow.Keys.state_key(batch_id, partition_key)
        original_indices = Enum.map(group, fn {idx, _attrs} -> idx end)
        command_attrs = flow_many_command_attrs(command, attrs)
        command_attrs = flow_stamp_shard(command_attrs, shard_idx)
        {shard_idx, key, original_indices, {command, key, command_attrs}}
      end)

    case Enum.find(groups, fn {_shard_idx, key, _indices, _cmd} ->
           byte_size(key) > @max_key_size
         end) do
      {_shard_idx, _key, indices, _cmd} ->
        error = {:error, "ERR key too large (max #{@max_key_size} bytes)"}
        {:ok, flow_many_error_results(count, indices, error)}

      nil ->
        keyed_commands = Enum.map(groups, fn {_shard_idx, key, _indices, cmd} -> {key, cmd} end)
        group_results = batch_quorum_commands(ctx, keyed_commands)
        expand_flow_many_results(count, groups, group_results)
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
        |> Enum.map(fn idx -> Map.get(expanded, idx, ErrorReasons.write_timeout_unknown()) end)

      {:ok, results}
    end
  end

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

        _unknown_or_non_empty ->
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
        filtered =
          Enum.reject(partition_keys, fn partition_key ->
            flow_claim_due_empty_precheck(
              ctx,
              idx,
              type,
              requested_state,
              priority,
              partition_key,
              attrs
            ) ==
              :empty
          end)

        if filtered == [], do: [], else: [{idx, filtered}]
      end)

    group_count = length(groups)

    if group_count == 0 do
      []
    else
      base = div(limit, group_count)
      extra = rem(limit, group_count)

      groups
      |> Enum.with_index()
      |> Enum.flat_map(fn {{_idx, partition_keys}, group_idx} ->
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

          [{key, {:flow_claim_due, key, shard_attrs}}]
        end
      end)
    end
  end

  defp flow_claim_due_empty_precheck(ctx, idx, type, state, priority, partition_key, attrs) do
    with true <- flow_claim_due_empty_precheck_allowed?(ctx, idx),
         {:ok, due_keys} <- flow_claim_due_precheck_keys(type, state, priority, partition_key),
         true <- due_keys != [],
         {:ok, native} <- direct_flow_index_read(ctx, idx, & &1) do
      now_ms = flow_claim_due_precheck_now_ms(attrs)

      case NativeFlowIndex.due_keys_present(native, due_keys, now_ms) do
        [] -> :empty
        [_ | _] -> :non_empty
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

  defp flow_claim_due_auto_priorities(_ctx, _attrs, _start_idx, _priorities, limit, acc, count)
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

  defp flow_claim_due_any_priorities(ctx, attrs, start_idx, [priority | rest], limit, acc, count) do
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
    parent_partition = Map.get(record, :parent_partition_key) || Map.get(record, :partition_key)

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
    {buckets, count} =
      Enum.reduce(attrs_list, {flow_fixed_shard_buckets(ctx.shard_count), 0}, fn
        %{id: id} = attrs, {buckets, index} ->
          key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))
          shard_idx = shard_for(ctx, key)

          {flow_put_shard_bucket(buckets, shard_idx, {index, key, attrs}), index + 1}
      end)

    groups =
      flow_nonempty_shard_buckets(buckets, ctx.shard_count, fn shard_idx, entries ->
        entries = Enum.reverse(entries)
        {indices, keys, attrs} = flow_create_pipeline_entries(entries, [], [], [])
        route_key = List.first(keys)

        {shard_idx, route_key, indices,
         {:flow_create_pipeline_batch, route_key, flow_stamp_shard(%{records: attrs}, shard_idx)}}
      end)

    keyed_commands = Enum.map(groups, fn {_shard_idx, key, _indices, cmd} -> {key, cmd} end)
    group_results = batch_quorum_commands(ctx, keyed_commands)

    results =
      groups
      |> Enum.zip(group_results)
      |> Enum.reduce(flow_result_tuple(count), fn {{_shard_idx, _route_key, indices, _cmd},
                                                   result},
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

  defp flow_result_tuple(count) when is_integer(count) and count >= 0 do
    :erlang.make_tuple(count, ErrorReasons.write_timeout_unknown())
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
      |> Enum.reduce({flow_fixed_shard_buckets(ctx.shard_count), 0}, fn {%{id: id} = attrs, index},
                                                                        {buckets, count} ->
        key = Ferricstore.Flow.Keys.state_key(id, Map.get(attrs, :partition_key))
        shard_idx = shard_for(ctx, key)

        {flow_put_shard_bucket(buckets, shard_idx, {index, key, attrs}), max(count, index + 1)}
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

  defp flow_terminal_pipeline_entries([{index, key, record_attrs} | rest], indices, keys, attrs) do
    flow_terminal_pipeline_entries(rest, [index | indices], [key | keys], [
      record_attrs | attrs
    ])
  end

  defp flow_terminal_command_batch([], ctx, batch_acc, result_acc) do
    flow_terminal_flush_batch(ctx, batch_acc, result_acc)
  end

  defp flow_terminal_command_batch([{op, %{id: id} = attrs} | rest], ctx, batch_acc, result_acc)
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
      forced_single_key_quorum(ctx, dest_key, {:pfmerge, dest_key, source_keys, source_sketches})
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

  @doc false
  def json_set(ctx, key, path, value, flags)
      when is_binary(key) and (is_binary(path) or is_list(path)) and is_binary(value) and
             is_list(flags) do
    forced_single_key_quorum(ctx, key, {:json_set, key, path, value, flags})
  end

  @doc false
  def json_del(ctx, key, path) when is_binary(key) and (is_binary(path) or is_list(path)) do
    forced_single_key_quorum(ctx, key, {:json_del, key, path})
  end

  @doc false
  def json_numincrby(ctx, key, path, increment)
      when is_binary(key) and (is_binary(path) or is_list(path)) and is_number(increment) do
    forced_single_key_quorum(ctx, key, {:json_numincrby, key, path, increment})
  end

  @doc false
  def json_arrappend(ctx, key, path, values)
      when is_binary(key) and (is_binary(path) or is_list(path)) and is_list(values) do
    forced_single_key_quorum(ctx, key, {:json_arrappend, key, path, values})
  end

  @doc false
  def json_toggle(ctx, key, path) when is_binary(key) and (is_binary(path) or is_list(path)) do
    forced_single_key_quorum(ctx, key, {:json_toggle, key, path})
  end

  @doc false
  def json_clear(ctx, key, path) when is_binary(key) and (is_binary(path) or is_list(path)) do
    forced_single_key_quorum(ctx, key, {:json_clear, key, path})
  end

  defp forced_single_key_quorum(ctx, key, command) do
    idx = shard_for(ctx, key)
    forced_quorum_write(ctx, idx, command, node())
  end

  @hll_wrongtype_error {:error,
                        "WRONGTYPE Operation against a key holding the wrong kind of value"}

  defp hll_read_sketches(ctx, keys) do
    with :ok <- hll_ensure_string_keys(ctx, keys) do
      ctx
      |> batch_get(keys)
      |> Enum.map(fn
        nil -> HLL.new()
        value -> value
      end)
      |> hll_validate_sketches()
    end
  end

  defp hll_ensure_string_keys(ctx, keys) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      if hll_compound_data_structure_key?(ctx, key) do
        {:halt, @hll_wrongtype_error}
      else
        {:cont, :ok}
      end
    end)
  end

  defp hll_compound_data_structure_key?(ctx, key) do
    compound_get(ctx, key, CompoundKey.type_key(key)) != nil and
      TypeRegistry.get_type(key, ctx) != "none"
  end

  defp hll_validate_sketches(sketches) do
    Enum.reduce_while(sketches, {:ok, []}, fn sketch, {:ok, acc} ->
      if HLL.valid_sketch?(sketch) do
        {:cont, {:ok, [sketch | acc]}}
      else
        {:halt, @hll_wrongtype_error}
      end
    end)
    |> case do
      {:ok, values} -> {:ok, Enum.reverse(values)}
      {:error, _} = err -> err
    end
  end

  @doc """
  Atomically applies Redis SET options in Raft order.

  Unlike `put/4`, this keeps NX/XX/GET/KEEPTTL checks inside the state
  machine so concurrent conditional SETs serialize correctly.
  """
  @spec set(FerricStore.Instance.t(), binary(), binary(), map()) :: term()
  def set(ctx, key, value, opts) do
    cond do
      byte_size(key) > @max_key_size ->
        {:error, "ERR key too large (max #{@max_key_size} bytes)"}

      is_binary(value) and byte_size(value) >= @max_value_size ->
        {:error, "ERR value too large (max #{@max_value_size} bytes)"}

      true ->
        case check_keydir_full_for_set(ctx, key, opts) do
          :ok ->
            idx = shard_for(ctx, key)
            raft_write(ctx, idx, key, {:set, key, value, opts.expire_at_ms, opts})

          {:error, _} = err ->
            err
        end
    end
  end

  # Checks if the keydir is full. If so, only allows writes to existing keys.
  # Checks both `keydir_full?` (ETS-level memory guard) and `reject_writes?`
  # (noeviction policy with reject-level pressure). The Shard GenServer has its
  # own `reject_writes?` check in `handle_call({:put, ...})`, but when the
  # quorum bypass path is used, the Shard is skipped, so we must check here.
  # Reads from ctx.pressure_flags atomics instead of persistent_term.
  defp check_keydir_full(ctx, key) do
    keydir_full = :atomics.get(ctx.pressure_flags, 1) == 1
    reject_writes = :atomics.get(ctx.pressure_flags, 2) == 1

    if keydir_full or reject_writes do
      # Allow updates to existing keys — use ETS direct check
      if exists_fast?(ctx, key) do
        :ok
      else
        # Nudge MemoryGuard to run eviction immediately (async, non-blocking).
        # Without this, the next eviction cycle is up to 100ms away.
        Ferricstore.MemoryGuard.nudge()
        {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
      end
    else
      :ok
    end
  end

  defp check_keydir_full_for_set(ctx, key, opts) do
    keydir_full = :atomics.get(ctx.pressure_flags, 1) == 1
    reject_writes = :atomics.get(ctx.pressure_flags, 2) == 1

    if keydir_full or reject_writes do
      existing? = exists_fast?(ctx, key)

      cond do
        existing? ->
          :ok

        opts.xx ->
          :ok

        true ->
          Ferricstore.MemoryGuard.nudge()
          {:error, "KEYDIR_FULL cannot accept new keys, keydir RAM limit reached"}
      end
    else
      :ok
    end
  end

  @doc "Deletes `key`. Returns `:ok` whether or not the key existed."
  @spec delete(FerricStore.Instance.t(), binary()) :: :ok
  def delete(ctx, key) do
    idx = shard_for(ctx, key)

    raft_write(ctx, idx, key, {:delete, key})
  end

  @doc """
  Submits a server command through Raft for replication to all nodes.

  Server commands are opaque to the library — the state machine dispatches
  them via the `raft_apply_hook` callback on the Instance struct. Routed
  through shard 0 for consistent ordering.
  """
  @spec server_command(FerricStore.Instance.t(), term()) :: term()
  def server_command(ctx, command) do
    raft_write(ctx, 0, "__server__", {:server_command, command})
  end

  @doc """
  Routes a probabilistic data structure write command through Raft.
  """
  @spec prob_write(FerricStore.Instance.t(), tuple()) :: term()
  def prob_write(ctx, command) do
    key = extract_prob_key(command)
    idx = shard_for(ctx, key)
    raft_write(ctx, idx, key, command)
  end

  defp extract_prob_key({:bloom_create, key, _, _, _}), do: key
  defp extract_prob_key({:bloom_add, key, _, _}), do: key
  defp extract_prob_key({:bloom_madd, key, _, _}), do: key
  defp extract_prob_key({:cms_create, key, _, _}), do: key
  defp extract_prob_key({:cms_incrby, key, _}), do: key
  defp extract_prob_key({:cms_merge, dst_key, _, _, _}), do: dst_key
  defp extract_prob_key({:cuckoo_create, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_add, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_addnx, key, _, _}), do: key
  defp extract_prob_key({:cuckoo_del, key, _}), do: key
  defp extract_prob_key({:topk_create, key, _, _, _, _}), do: key
  defp extract_prob_key({:topk_add, key, _}), do: key
  defp extract_prob_key({:topk_incrby, key, _}), do: key

  @doc """
  Returns `true` if `key` exists and is not expired.

  Uses direct ETS lookup (no GenServer roundtrip) for hot and cold keys.
  A key is considered existing if it is in the keydir and not expired,
  regardless of whether its value is hot (in ETS) or cold (on disk only).
  """
  @spec exists?(FerricStore.Instance.t(), binary()) :: boolean()
  def exists?(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, val, 0, _lfu, _fid, _off, _vsize}] when val != nil ->
          true

        [{^key, nil, 0, _lfu, fid, off, vsize}] when readable_cold_ref?(fid, off, vsize) ->
          true

        [{^key, val, exp, _lfu, _fid, _off, _vsize}] when exp > now and val != nil ->
          true

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and readable_cold_ref?(fid, off, vsize) ->
          true

        [{^key, _val, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          false

        [] ->
          false
      end
    rescue
      ArgumentError -> keydir_unavailable(ctx, idx, :exists, false)
    end
  end

  @doc """
  Fast ETS-direct existence check for a key.

  Returns `true` if the key exists in ETS and is not expired, `false` otherwise.
  This bypasses the GenServer entirely, saving ~1-3us per call. Used in the
  hot write path (`check_keydir_full/2`) where we only need a boolean answer
  and can tolerate the fact that cold keys (value=nil but still in keydir)
  are correctly detected as existing.
  """
  @spec exists_fast?(FerricStore.Instance.t(), binary()) :: boolean()
  def exists_fast?(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, val, 0, _lfu, _fid, _off, _vsize}] when val != nil ->
          true

        [{^key, nil, 0, _lfu, fid, off, vsize}] when readable_cold_ref?(fid, off, vsize) ->
          true

        [{^key, val, exp, _lfu, _fid, _off, _vsize}] when exp > now and val != nil ->
          true

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and readable_cold_ref?(fid, off, vsize) ->
          true

        [{^key, _val, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          false

        [] ->
          false
      end
    rescue
      ArgumentError -> false
    end
  end

  @doc """
  Atomically increments the integer value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_integer}`
  on success or `{:error, reason}` if the value is not a valid integer.
  """
  @spec incr(FerricStore.Instance.t(), binary(), integer()) ::
          {:ok, integer()} | {:error, binary()}
  def incr(ctx, key, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:incr, key, delta})
  end

  @doc """
  Atomically increments the float value of `key` by `delta`.

  If the key does not exist, it is set to `delta`. Returns `{:ok, new_float_string}`
  on success or `{:error, reason}` if the value is not a valid float.
  """
  @spec incr_float(FerricStore.Instance.t(), binary(), float()) ::
          {:ok, binary()} | {:error, binary()}
  def incr_float(ctx, key, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:incr_float, key, delta})
  end

  @doc """
  Atomically appends `suffix` to the value of `key`.

  If the key does not exist, it is created with value `suffix`.
  Returns `{:ok, new_byte_length}`.
  """
  @spec append(FerricStore.Instance.t(), binary(), binary()) :: {:ok, non_neg_integer()}
  def append(ctx, key, suffix) do
    raft_write(ctx, shard_for(ctx, key), key, {:append, key, suffix})
  end

  @doc """
  Atomically gets the old value and sets a new value for `key`.

  Returns the old value, or `nil` if the key did not exist.
  """
  @spec getset(FerricStore.Instance.t(), binary(), binary()) :: binary() | nil
  def getset(ctx, key, value) do
    raft_write(ctx, shard_for(ctx, key), key, {:getset, key, value})
  end

  @doc """
  Atomically gets and deletes `key`.

  Returns the value, or `nil` if the key did not exist.
  """
  @spec getdel(FerricStore.Instance.t(), binary()) :: binary() | nil
  def getdel(ctx, key) do
    raft_write(ctx, shard_for(ctx, key), key, {:getdel, key})
  end

  @doc """
  Atomically gets the value and updates the expiry of `key`.

  `expire_at_ms` is an absolute Unix-epoch timestamp in milliseconds;
  pass `0` to persist (remove expiry). Returns the value, or `nil` if
  the key did not exist.
  """
  @spec getex(FerricStore.Instance.t(), binary(), non_neg_integer()) :: binary() | nil
  def getex(ctx, key, expire_at_ms) do
    raft_write(ctx, shard_for(ctx, key), key, {:getex, key, expire_at_ms})
  end

  @doc """
  Atomically overwrites part of the string at `key` starting at `offset`.

  Zero-pads if the key doesn't exist or the string is shorter than offset.
  Returns `{:ok, new_byte_length}`.
  """
  @spec setrange(FerricStore.Instance.t(), binary(), non_neg_integer(), binary()) ::
          {:ok, non_neg_integer()}
  def setrange(ctx, key, offset, value) do
    raft_write(ctx, shard_for(ctx, key), key, {:setrange, key, offset, value})
  end

  @doc """
  Atomically sets the bit at `offset` to `bit_val` (0 or 1). Returns the
  previous bit value (0 or 1). Extends the bitmap with zero bytes if
  necessary. Goes through Raft so concurrent SETBITs on the same key
  never lose updates — the state machine is the sole mutator.
  """
  @spec setbit(FerricStore.Instance.t(), binary(), non_neg_integer(), 0 | 1) :: 0 | 1
  def setbit(ctx, key, offset, bit_val) do
    raft_write(ctx, shard_for(ctx, key), key, {:setbit, key, offset, bit_val})
  end

  @doc """
  Atomically increments the integer value of hash field `field` in `key` by
  `delta`. Returns `{:ok, new_int}` or `{:error, reason}`. Shares ordering
  with the parent hash's shard (routes by the hash's redis_key).
  """
  @spec hincrby(FerricStore.Instance.t(), binary(), binary(), integer()) ::
          integer() | {:error, binary()}
  def hincrby(ctx, key, field, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:hincrby, key, field, delta})
  end

  @doc """
  Atomically increments the float value of hash field `field` in `key` by
  `delta`. Returns the new value as a string, or `{:error, reason}`.
  """
  @spec hincrbyfloat(FerricStore.Instance.t(), binary(), binary(), float()) ::
          binary() | {:error, binary()}
  def hincrbyfloat(ctx, key, field, delta) do
    raft_write(ctx, shard_for(ctx, key), key, {:hincrbyfloat, key, field, delta})
  end

  @doc """
  Atomically increments the score of `member` in the sorted set at `key` by
  `increment`. Returns the new score as a string.
  """
  @spec zincrby(FerricStore.Instance.t(), binary(), number(), binary()) ::
          binary() | {:error, binary()}
  def zincrby(ctx, key, increment, member) do
    raft_write(ctx, shard_for(ctx, key), key, {:zincrby, key, increment, member})
  end

  @doc "Returns all live (non-expired, non-deleted) keys across every shard."
  @spec keys(FerricStore.Instance.t()) :: [binary()]
  def keys(ctx) do
    if selected_waraft_ctx?(ctx) do
      waraft_live_keys(ctx)
    else
      shard_live_keys(ctx)
    end
  end

  defp shard_live_keys(ctx) do
    sc = ctx.shard_count

    Enum.flat_map(0..(sc - 1), fn i ->
      case safe_read_call(ctx, i, :keys) do
        {:ok, keys} -> keys
        :unavailable -> []
      end
    end)
  end

  defp waraft_live_keys(ctx) do
    sc = ctx.shard_count
    now = HLC.now_ms()

    Enum.flat_map(0..(sc - 1), fn i ->
      live_keydir_keys(ctx, i, resolve_keydir(ctx, i), now)
    end)
  end

  defp live_keydir_keys(ctx, idx, keydir, now) do
    {live_keys, expired_keys} =
      :ets.foldl(
        fn
          {key, value, 0, _lfu, _fid, _off, _vsize}, {live, expired} when value != nil ->
            {[key | live], expired}

          {key, nil, 0, _lfu, fid, off, vsize}, {live, expired}
          when readable_cold_ref?(fid, off, vsize) ->
            {[key | live], expired}

          {key, value, exp, _lfu, _fid, _off, _vsize}, {live, expired}
          when exp > now and value != nil ->
            {[key | live], expired}

          {key, nil, exp, _lfu, fid, off, vsize}, {live, expired}
          when exp > now and readable_cold_ref?(fid, off, vsize) ->
            {[key | live], expired}

          {key, _value, _exp, _lfu, _fid, _off, _vsize}, {live, expired} ->
            {live, [key | expired]}
        end,
        {[], []},
        keydir
      )

    Enum.each(expired_keys, fn key ->
      track_keydir_binary_delete(ctx, idx, keydir, key)
      :ets.delete(keydir, key)
    end)

    live_keys
  rescue
    ArgumentError ->
      keydir_unavailable(ctx, idx, :keys, [])
  end

  @doc "Returns the count of all live keys across every shard."
  @spec dbsize(FerricStore.Instance.t()) :: non_neg_integer()
  def dbsize(ctx) do
    sc = ctx.shard_count
    now = HLC.now_ms()

    Enum.reduce(0..(sc - 1), 0, fn i, acc ->
      acc + live_keydir_size(ctx, i, resolve_keydir(ctx, i), now)
    end)
  end

  defp live_keydir_size(ctx, idx, keydir, now) do
    {count, expired_keys} =
      :ets.foldl(
        fn
          {_key, _value, 0, _lfu, _fid, _off, _vsize}, {count, expired_keys} ->
            {count + 1, expired_keys}

          {_key, _value, exp, _lfu, _fid, _off, _vsize}, {count, expired_keys} when exp > now ->
            {count + 1, expired_keys}

          {key, _value, _exp, _lfu, _fid, _off, _vsize}, {count, expired_keys} ->
            {count, [key | expired_keys]}
        end,
        {0, []},
        keydir
      )

    Enum.each(expired_keys, fn key ->
      track_keydir_binary_delete(ctx, idx, keydir, key)
      :ets.delete(keydir, key)
    end)

    count
  rescue
    ArgumentError ->
      keydir_unavailable(ctx, idx, :dbsize, 0)
  end

  defp keydir_unavailable(ctx, idx, request, fallback) do
    emit_shard_unavailable(ctx, idx, request, :keydir_unavailable)
    fallback
  end

  @doc """
  Returns the current write version of the shard that owns `key`.

  Used by the WATCH/EXEC transaction mechanism to detect concurrent modifications.
  """
  @spec get_version(FerricStore.Instance.t(), binary()) :: non_neg_integer()
  def get_version(ctx, key) do
    idx = shard_for(ctx, key)

    if selected_waraft_ctx?(ctx) do
      shared_write_version(ctx, idx)
    else
      case safe_read_call(ctx, idx, {:get_version, key}) do
        {:ok, version} -> version
        :unavailable -> shared_write_version(ctx, idx)
      end
    end
  end

  defp shared_write_version(%{write_version: write_version}, idx) do
    size = :counters.info(write_version).size
    if idx < size, do: :counters.get(write_version, idx + 1), else: 0
  rescue
    _ -> 0
  end

  @doc """
  Returns a lightweight WATCH token for `key`.

  Hot keys use the value hash plus their live Bitcask location. Cold keys use
  their live keydir location and expiry, avoiding a large Bitcask read just to
  snapshot WATCH state. Pending entries fall back to the shard write version.
  """
  @spec watch_token(FerricStore.Instance.t(), binary()) :: term()
  def watch_token(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{^key, value, 0, _lfu, fid, off, vsize}]
        when value != nil and valid_cold_location(fid, off, vsize) ->
          {:hot, :erlang.phash2(value), fid, off, vsize, 0}

        [{^key, value, 0, _lfu, fid, off, vsize}]
        when value != nil and valid_waraft_segment_location(fid, off, vsize) ->
          {:hot, :erlang.phash2(value), fid, off, vsize, 0}

        [{^key, value, 0, _lfu, :pending, _off, _vsize}] when value != nil ->
          {:version, get_version(ctx, key)}

        [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize, 0}

        [{^key, nil, 0, _lfu, fid, off, vsize}]
        when valid_waraft_segment_location(fid, off, vsize) ->
          {:cold, fid, off, vsize, 0}

        [{^key, nil, 0, _lfu, :pending, _off, _vsize}] ->
          {:version, get_version(ctx, key)}

        [{^key, value, exp, _lfu, fid, off, vsize}]
        when exp > now and value != nil and valid_cold_location(fid, off, vsize) ->
          {:hot, :erlang.phash2(value), fid, off, vsize, exp}

        [{^key, value, exp, _lfu, fid, off, vsize}]
        when exp > now and value != nil and valid_waraft_segment_location(fid, off, vsize) ->
          {:hot, :erlang.phash2(value), fid, off, vsize, exp}

        [{^key, value, exp, _lfu, :pending, _off, _vsize}] when exp > now and value != nil ->
          {:version, get_version(ctx, key)}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          {:cold, fid, off, vsize, exp}

        [{^key, nil, exp, _lfu, fid, off, vsize}]
        when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
          {:cold, fid, off, vsize, exp}

        [{^key, nil, exp, _lfu, :pending, _off, _vsize}] when exp > now ->
          {:version, get_version(ctx, key)}

        [{^key, _value, _exp, _lfu, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :missing

        [] ->
          :missing
      end
    rescue
      ArgumentError -> {:version, get_version(ctx, key)}
    end
  end

  @doc """
  Returns the keydir disk location for a key, or `:miss`.

  Reads the `{file_id, offset, value_size}` fields directly from the keydir
  ETS table without a GenServer roundtrip. Returns `{:ok, {fid, off, vsize}}`
  for live keys, or `:miss` if the key is not in the keydir or is expired.

  Used by sendfile zero-copy and STRLEN on cold keys.
  """
  @spec get_keydir_file_ref(FerricStore.Instance.t(), binary()) ::
          {:ok, {non_neg_integer(), non_neg_integer(), non_neg_integer()}} | :miss
  def get_keydir_file_ref(ctx, key) do
    idx = shard_for(ctx, key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    try do
      case :ets.lookup(keydir, key) do
        [{_, _, 0, _, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
          {:ok, {fid, off, vsize}}

        [{^key, nil, 0, _, fid, _off, _vsize}] when is_integer(fid) ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :miss

        [{_, _, 0, _, _fid, _off, _vsize}] ->
          :miss

        [{_, _, exp, _, fid, off, vsize}]
        when exp > now and valid_cold_location(fid, off, vsize) ->
          {:ok, {fid, off, vsize}}

        [{^key, nil, exp, _, fid, _off, _vsize}] when exp > now and is_integer(fid) ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :miss

        [{_, _, exp, _, _fid, _off, _vsize}] when exp > now ->
          :miss

        [{^key, _, _exp, _, _fid, _off, _vsize}] ->
          track_keydir_binary_delete(ctx, idx, keydir, key)
          :ets.delete(keydir, key)
          :miss

        [] ->
          :miss
      end
    rescue
      ArgumentError -> :miss
    end
  end

  # -------------------------------------------------------------------
  # Native command accessors
  # -------------------------------------------------------------------

  @spec cas(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer() | nil) ::
          1 | 0 | nil
  def cas(ctx, key, expected, new_value, ttl_ms) do
    expire_at_ms = if ttl_ms, do: HLC.now_ms() + ttl_ms, else: nil
    raft_write(ctx, shard_for(ctx, key), key, {:cas, key, expected, new_value, expire_at_ms})
  end

  @spec lock(FerricStore.Instance.t(), binary(), binary(), pos_integer()) ::
          :ok | {:error, binary()}
  def lock(ctx, key, owner, ttl_ms) do
    expire_at_ms = HLC.now_ms() + ttl_ms
    raft_write(ctx, shard_for(ctx, key), key, {:lock, key, owner, expire_at_ms})
  end

  @spec unlock(FerricStore.Instance.t(), binary(), binary()) :: 1 | {:error, binary()}
  def unlock(ctx, key, owner) do
    raft_write(ctx, shard_for(ctx, key), key, {:unlock, key, owner})
  end

  @spec extend(FerricStore.Instance.t(), binary(), binary(), pos_integer()) ::
          1 | {:error, binary()}
  def extend(ctx, key, owner, ttl_ms) do
    expire_at_ms = HLC.now_ms() + ttl_ms
    raft_write(ctx, shard_for(ctx, key), key, {:extend, key, owner, expire_at_ms})
  end

  @spec ratelimit_add(
          FerricStore.Instance.t(),
          binary(),
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) :: [term()]
  def ratelimit_add(ctx, key, window_ms, max, count) do
    raft_write(
      ctx,
      shard_for(ctx, key),
      key,
      {:ratelimit_add, key, window_ms, max, count}
    )
  end

  # -------------------------------------------------------------------
  # Compound key operations
  # -------------------------------------------------------------------

  @spec compound_get(FerricStore.Instance.t(), binary(), binary()) :: binary() | nil
  def compound_get(ctx, redis_key, compound_key) do
    idx = shard_for(ctx, redis_key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, compound_key, now) do
      {:hit, value, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
        value

      {:cold, file_id, offset, value_size}
      when valid_waraft_segment_location(file_id, offset, value_size) ->
        case read_waraft_segment_materialized(ctx, idx, file_id, compound_key) do
          {:ok, value} ->
            Stats.record_cold_read(ctx, compound_key)
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            value

          _ ->
            fallback_compound_get(ctx, idx, redis_key, compound_key)
        end

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_materialized(ctx, idx, path, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, compound_key)
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            value

          _ ->
            retry_or_fallback_compound_get(
              ctx,
              idx,
              keydir,
              redis_key,
              compound_key,
              {file_id, offset, value_size},
              now
            )
        end

      _ ->
        fallback_compound_get(ctx, idx, redis_key, compound_key)
    end
  end

  @spec compound_batch_get(FerricStore.Instance.t(), binary(), [binary()]) :: [binary() | nil]
  def compound_batch_get(ctx, redis_key, compound_keys) do
    idx = shard_for(ctx, redis_key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    {results, {fallback_keys, hot_hits}} =
      Enum.map_reduce(compound_keys, {[], []}, fn compound_key, {fallback_keys, hot_hits} ->
        case ets_get_full(ctx, idx, keydir, compound_key, now) do
          {:hit, value, lfu} ->
            {{:value, value}, {fallback_keys, [{keydir, compound_key, lfu} | hot_hits]}}

          {:cold, file_id, offset, value_size}
          when valid_cold_location(file_id, offset, value_size) or
                 valid_waraft_segment_location(file_id, offset, value_size) ->
            case direct_waraft_compound_cold_get(
                   ctx,
                   idx,
                   keydir,
                   compound_key,
                   file_id,
                   offset,
                   value_size,
                   now
                 ) do
              {:ok, value} -> {{:value, value}, {fallback_keys, hot_hits}}
              :fallback -> {:fallback, {[compound_key | fallback_keys], hot_hits}}
            end

          _ ->
            {:fallback, {[compound_key | fallback_keys], hot_hits}}
        end
      end)

    sampled_read_bookkeeping_batch(ctx, Enum.reverse(hot_hits), length(hot_hits))

    fallback_values =
      case fallback_keys do
        [] ->
          []

        keys ->
          pending_keys = Enum.reverse(keys)

          case safe_read_call(ctx, idx, {:compound_batch_get, redis_key, pending_keys}) do
            {:ok, values} -> values
            :unavailable -> List.duplicate(nil, length(pending_keys))
          end
      end

    {values, []} =
      Enum.map_reduce(results, fallback_values, fn
        {:value, value}, remaining -> {value, remaining}
        :fallback, [value | remaining] -> {value, remaining}
      end)

    values
  end

  defp retry_or_fallback_compound_get(
         ctx,
         idx,
         keydir,
         redis_key,
         compound_key,
         original_location,
         now
       ) do
    case retry_changed_cold_value(ctx, idx, keydir, compound_key, original_location, now) do
      {:cold, value, retry_file_id, retry_offset} ->
        Stats.record_cold_read(ctx, compound_key)

        warm_ets_after_cold_read(
          ctx,
          idx,
          keydir,
          compound_key,
          value,
          retry_file_id,
          retry_offset
        )

        value

      {:hot, value} ->
        value

      :miss ->
        fallback_compound_get(ctx, idx, redis_key, compound_key)
    end
  end

  defp fallback_compound_get(ctx, idx, redis_key, compound_key) do
    case safe_read_call(ctx, idx, {:compound_get, redis_key, compound_key}) do
      {:ok, value} -> value
      :unavailable -> nil
    end
  end

  defp direct_waraft_compound_cold_get(
         ctx,
         idx,
         keydir,
         compound_key,
         file_id,
         offset,
         value_size,
         now
       ) do
    case read_compound_cold_materialized(ctx, idx, file_id, offset, compound_key) do
      {:ok, value} when is_binary(value) ->
        Stats.record_cold_read(ctx, compound_key)
        warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
        {:ok, value}

      _ ->
        case retry_changed_cold_value(
               ctx,
               idx,
               keydir,
               compound_key,
               {file_id, offset, value_size},
               now
             ) do
          {:cold, value, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, compound_key)

            warm_ets_after_cold_read(
              ctx,
              idx,
              keydir,
              compound_key,
              value,
              retry_file_id,
              retry_offset
            )

            {:ok, value}

          {:hot, value} ->
            {:ok, value}

          :miss ->
            :fallback
        end
    end
  end

  defp read_compound_cold_materialized(
         ctx,
         idx,
         file_id,
         _offset,
         key
       )
       when is_tuple(file_id) and tuple_size(file_id) == 2 and
              (elem(file_id, 0) == :waraft_segment or
                 elem(file_id, 0) == :waraft_projection or
                 elem(file_id, 0) == :waraft_apply_projection) and
              is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0,
       do: read_waraft_segment_materialized(ctx, idx, file_id, key)

  defp read_compound_cold_materialized(ctx, idx, file_id, offset, key) do
    path = cold_file_path(ctx, idx, file_id)
    read_cold_materialized(ctx, idx, path, offset, key)
  end

  @spec compound_get_meta(FerricStore.Instance.t(), binary(), binary()) ::
          {binary(), non_neg_integer()} | nil
  def compound_get_meta(ctx, redis_key, compound_key) do
    idx = shard_for(ctx, redis_key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    case ets_get_meta_full(ctx, idx, keydir, compound_key, now) do
      {:hit, value, expire_at_ms, lfu} ->
        sampled_read_bookkeeping_fast(ctx, keydir, compound_key, lfu)
        {value, expire_at_ms}

      {:cold, file_id, offset, value_size, expire_at_ms}
      when valid_cold_location(file_id, offset, value_size) or
             valid_waraft_segment_location(file_id, offset, value_size) ->
        case read_compound_cold_materialized(ctx, idx, file_id, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            Stats.record_cold_read(ctx, compound_key)
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            {value, expire_at_ms}

          _ ->
            retry_or_fallback_compound_get_meta(
              ctx,
              idx,
              keydir,
              redis_key,
              compound_key,
              {file_id, offset, value_size},
              now
            )
        end

      _ ->
        fallback_compound_get_meta(ctx, idx, redis_key, compound_key)
    end
  end

  @spec compound_batch_get_meta(FerricStore.Instance.t(), binary(), [binary()]) ::
          [{binary(), non_neg_integer()} | nil]
  def compound_batch_get_meta(ctx, redis_key, compound_keys) do
    idx = shard_for(ctx, redis_key)
    keydir = resolve_keydir(ctx, idx)
    now = HLC.now_ms()

    {results, {fallback_keys, hot_hits}} =
      Enum.map_reduce(compound_keys, {[], []}, fn compound_key, {fallback_keys, hot_hits} ->
        case ets_get_meta_full(ctx, idx, keydir, compound_key, now) do
          {:hit, value, expire_at_ms, lfu} ->
            {{:value, {value, expire_at_ms}},
             {fallback_keys, [{keydir, compound_key, lfu} | hot_hits]}}

          {:cold, file_id, offset, value_size, expire_at_ms}
          when readable_cold_ref?(file_id, offset, value_size) ->
            case direct_waraft_compound_cold_get_meta(
                   ctx,
                   idx,
                   keydir,
                   compound_key,
                   file_id,
                   offset,
                   value_size,
                   now,
                   expire_at_ms
                 ) do
              {:ok, meta} -> {{:value, meta}, {fallback_keys, hot_hits}}
              :fallback -> {:fallback, {[compound_key | fallback_keys], hot_hits}}
            end

          _ ->
            {:fallback, {[compound_key | fallback_keys], hot_hits}}
        end
      end)

    sampled_read_bookkeeping_batch(ctx, Enum.reverse(hot_hits), length(hot_hits))

    fallback_values =
      case fallback_keys do
        [] ->
          []

        keys ->
          pending_keys = Enum.reverse(keys)

          case safe_read_call(ctx, idx, {:compound_batch_get_meta, redis_key, pending_keys}) do
            {:ok, metas} -> metas
            :unavailable -> List.duplicate(nil, length(pending_keys))
          end
      end

    {values, []} =
      Enum.map_reduce(results, fallback_values, fn
        {:value, value}, remaining -> {value, remaining}
        :fallback, [value | remaining] -> {value, remaining}
      end)

    values
  end

  defp direct_waraft_compound_cold_get_meta(
         ctx,
         idx,
         keydir,
         compound_key,
         file_id,
         offset,
         value_size,
         now,
         expire_at_ms
       ) do
    case read_compound_cold_materialized(ctx, idx, file_id, offset, compound_key) do
      {:ok, value} when is_binary(value) ->
        Stats.record_cold_read(ctx, compound_key)
        warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
        {:ok, {value, expire_at_ms}}

      _ ->
        case retry_changed_cold_meta(
               ctx,
               idx,
               keydir,
               compound_key,
               {file_id, offset, value_size},
               now
             ) do
          {:cold, value, retry_expire_at_ms, retry_file_id, retry_offset} ->
            Stats.record_cold_read(ctx, compound_key)

            warm_ets_after_cold_read(
              ctx,
              idx,
              keydir,
              compound_key,
              value,
              retry_file_id,
              retry_offset
            )

            {:ok, {value, retry_expire_at_ms}}

          {:hot, value, retry_expire_at_ms} ->
            {:ok, {value, retry_expire_at_ms}}

          :miss ->
            :fallback
        end
    end
  end

  defp retry_or_fallback_compound_get_meta(
         ctx,
         idx,
         keydir,
         redis_key,
         compound_key,
         original_location,
         now
       ) do
    case retry_changed_cold_meta(ctx, idx, keydir, compound_key, original_location, now) do
      {:cold, value, expire_at_ms, retry_file_id, retry_offset} ->
        Stats.record_cold_read(ctx, compound_key)

        warm_ets_after_cold_read(
          ctx,
          idx,
          keydir,
          compound_key,
          value,
          retry_file_id,
          retry_offset
        )

        {value, expire_at_ms}

      {:hot, value, expire_at_ms} ->
        {value, expire_at_ms}

      :miss ->
        fallback_compound_get_meta(ctx, idx, redis_key, compound_key)
    end
  end

  defp fallback_compound_get_meta(ctx, idx, redis_key, compound_key) do
    case safe_read_call(ctx, idx, {:compound_get_meta, redis_key, compound_key}) do
      {:ok, meta} -> meta
      :unavailable -> nil
    end
  end

  @spec compound_put(FerricStore.Instance.t(), binary(), binary(), binary(), non_neg_integer()) ::
          :ok | {:error, term()}
  def compound_put(ctx, redis_key, compound_key, value, expire_at_ms) do
    idx = shard_for(ctx, redis_key)

    if durable_raft_ctx?(ctx) do
      quorum_write(ctx, idx, CompoundCommand.put(compound_key, value, expire_at_ms))
    else
      safe_write_call(ctx, idx, {:compound_put, redis_key, compound_key, value, expire_at_ms})
    end
  end

  @spec compound_batch_put(
          FerricStore.Instance.t(),
          binary(),
          [{binary(), binary(), non_neg_integer()}]
        ) :: :ok | {:error, term()}
  def compound_batch_put(_ctx, _redis_key, []), do: :ok

  def compound_batch_put(ctx, redis_key, entries) do
    idx = shard_for(ctx, redis_key)

    if durable_raft_ctx?(ctx) do
      ctx
      |> quorum_write(idx, CompoundCommand.batch_put(redis_key, entries))
      |> normalize_compound_batch_write_result()
    else
      safe_write_call(ctx, idx, {:compound_batch_put, redis_key, entries})
    end
  end

  @spec compound_delete(FerricStore.Instance.t(), binary(), binary()) :: :ok | {:error, term()}
  def compound_delete(ctx, redis_key, compound_key) do
    idx = shard_for(ctx, redis_key)

    if durable_raft_ctx?(ctx) do
      quorum_write(ctx, idx, CompoundCommand.delete(compound_key))
    else
      safe_write_call(ctx, idx, {:compound_delete, redis_key, compound_key})
    end
  end

  @spec compound_batch_delete(FerricStore.Instance.t(), binary(), [binary()]) ::
          :ok | {:error, term()}
  def compound_batch_delete(_ctx, _redis_key, []), do: :ok

  def compound_batch_delete(ctx, redis_key, compound_keys) do
    idx = shard_for(ctx, redis_key)

    if durable_raft_ctx?(ctx) do
      ctx
      |> quorum_write(idx, CompoundCommand.batch_delete(redis_key, compound_keys))
      |> normalize_compound_batch_write_result()
    else
      safe_write_call(ctx, idx, {:compound_batch_delete, redis_key, compound_keys})
    end
  end

  defp normalize_compound_batch_write_result(:ok), do: :ok
  defp normalize_compound_batch_write_result({:error, _reason} = error), do: error

  defp normalize_compound_batch_write_result({:ok, results}),
    do: normalize_compound_batch_results(results)

  defp normalize_compound_batch_write_result(results) when is_list(results),
    do: normalize_compound_batch_results(results)

  defp normalize_compound_batch_write_result(other), do: other

  defp normalize_compound_batch_results(results) when is_list(results) do
    Enum.find(results, :ok, &match?({:error, _reason}, &1))
  end

  defp origin_compound_get(ctx, idx, keydir, compound_key) do
    now = HLC.now_ms()

    case ets_get_full(ctx, idx, keydir, compound_key, now) do
      {:hit, value, _lfu} ->
        value

      {:cold, file_id, offset, value_size}
      when valid_cold_location(file_id, offset, value_size) ->
        path = cold_file_path(ctx, idx, file_id)

        case read_cold_materialized(ctx, idx, path, offset, compound_key) do
          {:ok, value} when is_binary(value) ->
            warm_ets_after_cold_read(ctx, idx, keydir, compound_key, value, file_id, offset)
            value

          _ ->
            case retry_changed_cold_value(
                   ctx,
                   idx,
                   keydir,
                   compound_key,
                   {file_id, offset, value_size},
                   now
                 ) do
              {:cold, value, retry_file_id, retry_offset} ->
                warm_ets_after_cold_read(
                  ctx,
                  idx,
                  keydir,
                  compound_key,
                  value,
                  retry_file_id,
                  retry_offset
                )

                value

              {:hot, value} ->
                value

              :miss ->
                nil
            end
        end

      _ ->
        nil
    end
  end

  @spec compound_scan(FerricStore.Instance.t(), binary(), binary()) :: [{binary(), binary()}]
  def compound_scan(ctx, redis_key, prefix) do
    idx = shard_for(ctx, redis_key)

    if selected_waraft_ctx?(ctx) do
      idx
      |> direct_compound_scan(ctx, prefix)
      |> Enum.sort_by(fn {field, _} -> field end)
    else
      case safe_read_call(ctx, idx, {:compound_scan, redis_key, prefix}) do
        {:ok, results} -> results
        :unavailable -> []
      end
    end
  end

  @spec compound_fields(FerricStore.Instance.t(), binary(), binary()) :: [binary()]
  def compound_fields(ctx, redis_key, prefix) do
    idx = shard_for(ctx, redis_key)

    if selected_waraft_ctx?(ctx) do
      ctx
      |> direct_compound_fields(idx, prefix)
      |> Enum.sort()
    else
      case safe_read_call(ctx, idx, {:compound_fields, redis_key, prefix}) do
        {:ok, fields} -> fields
        :unavailable -> []
      end
    end
  end

  @spec compound_count(FerricStore.Instance.t(), binary(), binary()) :: non_neg_integer()
  def compound_count(ctx, redis_key, prefix) do
    idx = shard_for(ctx, redis_key)

    if selected_waraft_ctx?(ctx) do
      ctx
      |> resolve_keydir(idx)
      |> Ferricstore.Store.Shard.ETS.prefix_count_entries(prefix)
    else
      case safe_read_call(ctx, idx, {:compound_count, redis_key, prefix}) do
        {:ok, count} -> count
        :unavailable -> 0
      end
    end
  end

  defp direct_compound_scan(idx, ctx, prefix) do
    shard_data_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, idx)

    ctx
    |> direct_compound_read_state(idx)
    |> Ferricstore.Store.Shard.ETS.prefix_scan_entries(prefix, shard_data_path)
  end

  defp direct_compound_fields(ctx, idx, prefix) do
    ctx
    |> direct_compound_read_state(idx)
    |> Ferricstore.Store.Shard.ETS.prefix_scan_fields(prefix)
  end

  defp direct_compound_read_state(ctx, idx) do
    %{
      keydir: resolve_keydir(ctx, idx),
      data_dir: ctx.data_dir,
      index: idx,
      instance_ctx: ctx
    }
  end

  @spec zset_score_range(FerricStore.Instance.t(), binary(), term(), term(), boolean()) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_score_range(ctx, redis_key, min_bound, max_bound, reverse?) do
    idx = shard_for(ctx, redis_key)

    if selected_waraft_ctx?(ctx) do
      {:ok, direct_zset_score_range(ctx, idx, redis_key, min_bound, max_bound, reverse?)}
    else
      ctx
      |> safe_read_call(idx, {:zset_score_range, redis_key, min_bound, max_bound, reverse?})
      |> unwrap_zset_index_reply()
    end
  end

  @spec zset_score_range_slice(
          FerricStore.Instance.t(),
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) ::
          {:ok, [{binary(), float()}]} | :unavailable
  def zset_score_range_slice(ctx, redis_key, min_bound, max_bound, reverse?, offset, count) do
    idx = shard_for(ctx, redis_key)

    if selected_waraft_ctx?(ctx) do
      {:ok,
       ctx
       |> direct_zset_score_range(idx, redis_key, min_bound, max_bound, reverse?)
       |> apply_zset_slice(offset, count)}
    else
      ctx
      |> safe_read_call(
        idx,
        {:zset_score_range_slice, redis_key, min_bound, max_bound, reverse?, offset, count}
      )
      |> unwrap_zset_index_reply()
    end
  end

  @spec zset_score_count(FerricStore.Instance.t(), binary(), term(), term()) ::
          {:ok, non_neg_integer()} | :unavailable
  def zset_score_count(ctx, redis_key, min_bound, max_bound) do
    idx = shard_for(ctx, redis_key)

    if selected_waraft_ctx?(ctx) do
      {:ok, direct_zset_score_count(ctx, idx, redis_key, min_bound, max_bound)}
    else
      ctx
      |> safe_read_call(idx, {:zset_score_count, redis_key, min_bound, max_bound})
      |> unwrap_zset_index_reply()
    end
  end

  @spec zset_score_count_many(FerricStore.Instance.t(), [{binary(), term(), term()}]) ::
          {:ok, [non_neg_integer()]} | :unavailable
  def zset_score_count_many(_ctx, []), do: {:ok, []}

  def zset_score_count_many(ctx, [{first_key, _min, _max} | _] = queries) do
    if selected_waraft_ctx?(ctx) do
      {:ok,
       Enum.map(queries, fn {key, min_bound, max_bound} ->
         direct_zset_score_count(ctx, shard_for(ctx, key), key, min_bound, max_bound)
       end)}
    else
      idx = shard_for(ctx, first_key)

      if Enum.all?(queries, fn {key, _min_bound, _max_bound} -> shard_for(ctx, key) == idx end) do
        ctx
        |> safe_read_call(idx, {:zset_score_count_many, queries})
        |> unwrap_zset_index_reply()
      else
        zset_score_count_many_cross_shard(ctx, queries)
      end
    end
  end

  defp zset_score_count_many_cross_shard(ctx, queries) do
    Enum.reduce_while(queries, {:ok, []}, fn {key, min_bound, max_bound}, {:ok, acc} ->
      case zset_score_count(ctx, key, min_bound, max_bound) do
        {:ok, count} -> {:cont, {:ok, [count | acc]}}
        :unavailable -> {:halt, :unavailable}
      end
    end)
    |> case do
      {:ok, counts} -> {:ok, Enum.reverse(counts)}
      :unavailable -> :unavailable
    end
  end

  @spec zset_score_count_all_many_no_build(FerricStore.Instance.t(), [binary()]) ::
          {:ok, [non_neg_integer()]} | :unavailable
  def zset_score_count_all_many_no_build(_ctx, []), do: {:ok, []}

  def zset_score_count_all_many_no_build(ctx, [first_key | _] = keys) do
    if selected_waraft_ctx?(ctx) do
      {:ok,
       Enum.map(keys, fn key ->
         direct_zset_score_count(ctx, shard_for(ctx, key), key, :neg_inf, :inf)
       end)}
    else
      idx = shard_for(ctx, first_key)

      if Enum.all?(keys, fn key -> shard_for(ctx, key) == idx end) do
        ctx
        |> safe_read_call(idx, {:zset_score_count_all_many_no_build, keys})
        |> unwrap_zset_index_reply()
      else
        zset_score_count_all_many_no_build_cross_shard(ctx, keys)
      end
    end
  end

  defp zset_score_count_all_many_no_build_cross_shard(ctx, keys) do
    Enum.reduce_while(keys, {:ok, []}, fn key, {:ok, acc} ->
      idx = shard_for(ctx, key)

      case safe_read_call(ctx, idx, {:zset_score_count_all_many_no_build, [key]}) do
        {:ok, [count]} -> {:cont, {:ok, [count | acc]}}
        :unavailable -> {:halt, :unavailable}
      end
    end)
    |> case do
      {:ok, counts} -> {:ok, Enum.reverse(counts)}
      :unavailable -> :unavailable
    end
  end

  @spec flow_index_score_range_slice(
          FerricStore.Instance.t(),
          binary(),
          term(),
          term(),
          boolean(),
          non_neg_integer(),
          non_neg_integer() | :all
        ) :: {:ok, [{binary(), float()}]} | :unavailable
  def flow_index_score_range_slice(ctx, key, min_bound, max_bound, reverse?, offset, count) do
    idx = shard_for(ctx, key)

    if selected_waraft_ctx?(ctx) do
      direct_flow_index_score_range_slice(
        ctx,
        idx,
        key,
        min_bound,
        max_bound,
        reverse?,
        offset,
        count
      )
    else
      ctx
      |> safe_read_call(
        idx,
        {:flow_index_score_range_slice, key, min_bound, max_bound, reverse?, offset, count}
      )
      |> unwrap_zset_index_reply()
    end
  end

  @spec flow_index_rank_range(
          FerricStore.Instance.t(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          boolean()
        ) :: {:ok, [{binary(), float()}]} | :unavailable
  def flow_index_rank_range(ctx, key, start_idx, stop_idx, reverse?) do
    idx = shard_for(ctx, key)

    if selected_waraft_ctx?(ctx) do
      direct_flow_index_rank_range(ctx, idx, key, start_idx, stop_idx, reverse?)
    else
      ctx
      |> safe_read_call(idx, {:flow_index_rank_range, key, start_idx, stop_idx, reverse?})
      |> unwrap_zset_index_reply()
    end
  end

  @spec flow_index_rank_range_many(
          FerricStore.Instance.t(),
          [{binary(), non_neg_integer(), non_neg_integer(), boolean()}]
        ) :: {:ok, [[{binary(), float()}]]} | :unavailable
  def flow_index_rank_range_many(_ctx, []), do: {:ok, []}

  def flow_index_rank_range_many(ctx, requests) when is_list(requests) do
    if selected_waraft_ctx?(ctx) do
      direct_flow_index_rank_range_many(ctx, requests)
    else
      requests
      |> Enum.with_index()
      |> Enum.group_by(fn {{key, _start_idx, _stop_idx, _reverse?}, _index} ->
        shard_for(ctx, key)
      end)
      |> Enum.reduce_while({:ok, %{}}, fn {idx, indexed_requests}, {:ok, acc} ->
        shard_requests = Enum.map(indexed_requests, fn {request, _index} -> request end)

        case safe_read_call(ctx, idx, {:flow_index_rank_range_many, shard_requests})
             |> unwrap_zset_index_reply() do
          {:ok, results} when is_list(results) ->
            indexed =
              indexed_requests
              |> Enum.zip(results)
              |> Enum.reduce(acc, fn {{_request, original_index}, result}, next_acc ->
                Map.put(next_acc, original_index, result)
              end)

            {:cont, {:ok, indexed}}

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
  end

  @spec flow_index_count_all(FerricStore.Instance.t(), binary()) ::
          {:ok, non_neg_integer()} | :unavailable
  def flow_index_count_all(ctx, key) do
    idx = shard_for(ctx, key)

    if selected_waraft_ctx?(ctx) do
      direct_flow_index_count_all(ctx, idx, key)
    else
      ctx
      |> safe_read_call(idx, {:flow_index_count_all, key})
      |> unwrap_zset_index_reply()
    end
  end

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
    Enum.reduce_while(requests, {:ok, []}, fn {key, start_idx, stop_idx, reverse?}, {:ok, acc} ->
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
