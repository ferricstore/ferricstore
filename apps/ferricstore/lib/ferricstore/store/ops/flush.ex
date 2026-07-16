defmodule Ferricstore.Store.Ops.Flush do
  @moduledoc false

  alias Ferricstore.Flow.Governance.LimitCache
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.SharedRefBackfill
  alias Ferricstore.Raft.Batcher
  alias Ferricstore.Store.Router

  @pause_timeout_ms 30_000
  @resume_timeout_ms 5_000

  def flush(ctx) do
    LimitCache.with_drained_cache(ctx, fn ->
      with_writes_paused(ctx, fn -> do_flush(ctx) end)
    end)
  end

  defp do_flush(ctx) do
    flush_epoch = Ferricstore.HLC.now()

    with :ok <- SharedRefBackfill.invalidate_verified!(ctx.name, ctx.shard_count),
         :ok <- validate_internal_keydirs(ctx),
         :ok <- flush_replicated_shards(ctx, flush_epoch),
         :ok <- clear_promoted_storage(ctx),
         :ok <- clear_flow_projection_storage(ctx) do
      clear_stream_tables(ctx)
      NativeFlowIndex.reset_all(ctx.name, ctx.shard_count)
      :ok
    end
  end

  defp validate_internal_keydirs(ctx) do
    shard_indexes(ctx.shard_count)
    |> Enum.reduce_while(:ok, fn shard_index, :ok ->
      keydir = elem(ctx.keydir_refs, shard_index)

      try do
        case :ets.info(keydir, :size) do
          size when is_integer(size) and size >= 0 ->
            {:cont, :ok}

          _unavailable ->
            raise ArgumentError, "keydir table is unavailable"
        end
      rescue
        error in ArgumentError ->
          {:halt, {:error, {:flush_internal_keydir_unavailable, error}}}
      end
    end)
  end

  defp with_writes_paused(ctx, fun) when is_function(fun, 0) do
    case pause_all_writes(ctx) do
      {:ok, pause_token} ->
        run_while_paused(ctx, pause_token, fun)

      {:error, _reason} = error ->
        error
    end
  end

  defp run_while_paused(ctx, pause_token, fun) do
    try do
      result = fun.()
      merge_resume_result(result, resume_all_writes(ctx, pause_token))
    rescue
      error ->
        _ = resume_all_writes(ctx, pause_token)
        reraise error, __STACKTRACE__
    catch
      kind, reason ->
        _ = resume_all_writes(ctx, pause_token)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp merge_resume_result(result, :ok), do: result

  defp merge_resume_result(:ok, {:error, reason}),
    do: {:error, {:flush_resume_failed, reason}}

  defp merge_resume_result({:error, flush_reason}, {:error, resume_reason}),
    do: {:error, {:flush_failed_and_resume_failed, flush_reason, resume_reason}}

  defp merge_resume_result(result, {:error, resume_reason}),
    do: {:error, {:flush_result_and_resume_failed, result, resume_reason}}

  defp pause_all_writes(ctx) do
    if Router.durable_context?(ctx) do
      pause_durable_writes(ctx)
    else
      pause_standalone_writes(ctx)
    end
  end

  defp pause_durable_writes(ctx) do
    durable_nodes()
    |> Enum.reduce_while({:ok, []}, fn target_node, {:ok, paused_nodes} ->
      case call_durable_pause(target_node, ctx.shard_count) do
        :ok ->
          {:cont, {:ok, [target_node | paused_nodes]}}

        {:error, reason} ->
          _ = resume_durable_nodes(Enum.reverse(paused_nodes), ctx.shard_count)
          {:halt, {:error, {:flush_pause_failed, target_node, reason}}}

        other ->
          _ = resume_durable_nodes(Enum.reverse(paused_nodes), ctx.shard_count)
          {:halt, {:error, {:flush_pause_failed, target_node, other}}}
      end
    end)
    |> case do
      {:ok, paused_nodes} -> {:ok, {:durable, Enum.reverse(paused_nodes)}}
      {:error, _reason} = error -> error
    end
  end

  defp call_durable_pause(target_node, shard_count) when target_node == node() do
    Batcher.pause_writes_for_sync_all(shard_count, @pause_timeout_ms)
  end

  defp call_durable_pause(target_node, shard_count) do
    :erpc.call(
      target_node,
      Batcher,
      :pause_writes_for_sync_all,
      [shard_count, @pause_timeout_ms],
      @pause_timeout_ms + 1_000
    )
  catch
    kind, reason -> {:error, {:remote_pause_failed, kind, reason}}
  end

  defp pause_standalone_writes(ctx) do
    ctx.shard_names
    |> Tuple.to_list()
    |> Enum.reduce_while({:ok, []}, fn shard, {:ok, paused_shards} ->
      case call_standalone_shard(shard, {:pause_writes}, @pause_timeout_ms) do
        :ok ->
          {:cont, {:ok, [shard | paused_shards]}}

        {:error, reason} ->
          _ = resume_standalone_shards(Enum.reverse(paused_shards))
          {:halt, {:error, {:flush_pause_failed, shard, reason}}}

        other ->
          _ = resume_standalone_shards(Enum.reverse(paused_shards))
          {:halt, {:error, {:flush_pause_failed, shard, other}}}
      end
    end)
    |> case do
      {:ok, paused_shards} -> {:ok, {:standalone, Enum.reverse(paused_shards)}}
      {:error, _reason} = error -> error
    end
  end

  defp resume_all_writes(ctx, {:durable, paused_nodes}) do
    resume_durable_nodes(paused_nodes, ctx.shard_count)
  end

  defp resume_all_writes(_ctx, {:standalone, paused_shards}) do
    resume_standalone_shards(paused_shards)
  end

  defp resume_durable_nodes(nodes, shard_count) do
    failures =
      Enum.reduce(nodes, [], fn target_node, failures ->
        case call_durable_resume(target_node, shard_count) do
          :ok -> failures
          other -> [{target_node, other} | failures]
        end
      end)

    case Enum.reverse(failures) do
      [] -> :ok
      failures -> {:error, {:durable_resume_failed, failures}}
    end
  end

  defp call_durable_resume(target_node, shard_count) when target_node == node() do
    Batcher.resume_writes_for_sync_all(shard_count, @resume_timeout_ms)
  end

  defp call_durable_resume(target_node, shard_count) do
    :erpc.call(
      target_node,
      Batcher,
      :resume_writes_for_sync_all,
      [shard_count, @resume_timeout_ms],
      @resume_timeout_ms + 1_000
    )
  catch
    kind, reason -> {:error, {:remote_resume_failed, kind, reason}}
  end

  defp resume_standalone_shards(shards) do
    failures =
      Enum.reduce(shards, [], fn shard, failures ->
        case call_standalone_shard(shard, {:resume_writes}, @resume_timeout_ms) do
          :ok -> failures
          other -> [{shard, other} | failures]
        end
      end)

    case Enum.reverse(failures) do
      [] -> :ok
      failures -> {:error, {:standalone_resume_failed, failures}}
    end
  end

  defp call_standalone_shard(shard, command, timeout) do
    GenServer.call(shard, command, timeout)
  catch
    :exit, reason -> {:error, {:shard_call_failed, reason}}
  end

  defp durable_nodes do
    [node() | Node.list()]
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp flush_replicated_shards(ctx, flush_epoch) do
    shard_indexes(ctx.shard_count)
    |> Enum.reduce_while(:ok, fn shard_index, :ok ->
      case flush_replicated_shard(ctx, shard_index, flush_epoch) do
        {:ok, deleted} when is_integer(deleted) and deleted >= 0 -> {:cont, :ok}
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:flush_shard_failed, shard_index, reason}}}
        other -> {:halt, {:error, {:flush_shard_failed, shard_index, other}}}
      end
    end)
  end

  defp flush_replicated_shard(ctx, shard_index, flush_epoch) do
    if Router.durable_context?(ctx) do
      Batcher.write_flush_shard_paused(shard_index, flush_epoch)
    else
      ctx.shard_names
      |> elem(shard_index)
      |> GenServer.call({:flush_shard_paused, flush_epoch}, :infinity)
    end
  catch
    :exit, reason -> {:error, {:flush_shard_call_failed, reason}}
  end

  defp clear_flow_projection_storage(ctx) do
    Ferricstore.Flow.LMDBWriter.suspend_all(ctx.name, ctx.shard_count, flush: false)

    try do
      with :ok <- discard_flow_history_projectors(ctx),
           :ok <- discard_flow_lmdb_writers(ctx),
           :ok <- Ferricstore.Flow.LMDB.clear_all(ctx.data_dir, ctx.shard_count),
           :ok <- clear_flow_history_dirs(ctx),
           :ok <- finalize_empty_flow_shards(ctx) do
        :ok
      end
    after
      Ferricstore.Flow.LMDBWriter.resume_all(ctx.name, ctx.shard_count)
    end
  end

  defp clear_promoted_storage(ctx) do
    dedicated_root = Path.join(ctx.data_dir, "dedicated")

    if Ferricstore.FS.exists?(dedicated_root) do
      with :ok <- remove_promoted_storage(dedicated_root),
           :ok <- fsync_promoted_storage_parent(ctx.data_dir) do
        :ok
      end
    else
      :ok
    end
  end

  defp remove_promoted_storage(dedicated_root) do
    case Ferricstore.FS.rm_rf(dedicated_root) do
      :ok -> :ok
      {:error, reason} -> {:error, {:flush_promoted_storage_failed, dedicated_root, reason}}
    end
  end

  defp fsync_promoted_storage_parent(data_dir) do
    case Ferricstore.Bitcask.NIF.v2_fsync_dir(data_dir) do
      :ok -> :ok
      {:error, reason} -> {:error, {:flush_promoted_storage_fsync_failed, data_dir, reason}}
    end
  end

  defp discard_flow_lmdb_writers(%{name: name, shard_count: shard_count})
       when is_atom(name) and is_integer(shard_count) and shard_count >= 0 do
    Ferricstore.Flow.LMDBWriter.discard_all(name, shard_count)
  end

  defp discard_flow_lmdb_writers(_ctx), do: :ok

  defp discard_flow_history_projectors(ctx) do
    shard_indexes(ctx.shard_count)
    |> Enum.reduce_while(:ok, fn shard_index, :ok ->
      case Ferricstore.Flow.HistoryProjector.discard(ctx, shard_index, 5_000) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp clear_flow_history_dirs(ctx) do
    shard_indexes(ctx.shard_count)
    |> Enum.reduce_while(:ok, fn shard_index, :ok ->
      history_dir =
        ctx.data_dir
        |> Ferricstore.DataDir.shard_data_path(shard_index)
        |> Ferricstore.Flow.HistoryProjector.history_dir()

      case Ferricstore.FS.rm_rf(history_dir) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp finalize_empty_flow_shards(ctx) do
    shard_indexes(ctx.shard_count)
    |> Enum.reduce_while(:ok, fn shard_index, :ok ->
      keydir = elem(ctx.keydir_refs, shard_index)
      {file_id, file_path, shard_path} = Ferricstore.Store.ActiveFile.get(ctx, shard_index)

      try do
        :ok =
          SharedRefBackfill.finalize_empty_shard!(
            shard_path,
            keydir,
            shard_index,
            ctx,
            active_file_id: file_id,
            active_file_path: file_path
          )

        {:cont, :ok}
      rescue
        error -> {:halt, {:error, {:flow_backfill_finalize_failed, shard_index, error}}}
      end
    end)
  end

  defp shard_indexes(0), do: []
  defp shard_indexes(shard_count), do: 0..(shard_count - 1)

  defp clear_stream_tables(ctx), do: Ferricstore.Stream.LocalState.clear(ctx)
end
