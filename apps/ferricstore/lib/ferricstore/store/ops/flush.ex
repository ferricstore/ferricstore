defmodule Ferricstore.Store.Ops.Flush do
  @moduledoc false

  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.{InternalKey, Keys, SharedRefBackfill}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops.Compound, as: CompoundOps
  alias Ferricstore.Store.Ops.Delete, as: DeleteOps
  alias Ferricstore.Store.Router

  @internal_delete_batch_size 512

  def flush(ctx) do
    with :ok <- SharedRefBackfill.invalidate_verified!(ctx.name, ctx.shard_count),
         :ok <- flush_keys(ctx, Router.keys(ctx)),
         :ok <- flush_internal_keys(ctx),
         :ok <- clear_flow_projection_storage(ctx) do
      clear_stream_tables()
      NativeFlowIndex.reset_all(ctx.name, ctx.shard_count)
      :ok
    end
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

  defp discard_flow_lmdb_writers(%{name: name, shard_count: shard_count})
       when is_atom(name) and is_integer(shard_count) and shard_count >= 0 do
    Ferricstore.Flow.LMDBWriter.discard_all(name, shard_count)
  end

  defp discard_flow_lmdb_writers(_ctx), do: :ok

  defp discard_flow_history_projectors(ctx) do
    0..max(ctx.shard_count - 1, -1)//1
    |> Enum.reduce_while(:ok, fn shard_index, :ok ->
      case Ferricstore.Flow.HistoryProjector.discard(ctx, shard_index, 5_000) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp clear_flow_history_dirs(ctx) do
    0..max(ctx.shard_count - 1, -1)//1
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

  defp flush_keys(store, keys) do
    keys
    |> CompoundKey.storage_logical_keys()
    |> Enum.reject(&backfill_metadata_key?(store, &1))
    |> Enum.reduce_while(:ok, fn key, :ok ->
      case flush_key(store, key) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
        other -> {:halt, {:error, {:flush_key_failed, key, other}}}
      end
    end)
  end

  defp flush_internal_keys(ctx) do
    Enum.reduce_while(0..max(ctx.shard_count - 1, -1)//1, :ok, fn shard_index, :ok ->
      keydir = elem(ctx.keydir_refs, shard_index)
      :ets.safe_fixtable(keydir, true)

      result =
        try do
          match_spec = [{{:"$1", :_, :_, :_, :_, :_, :_}, [{:is_binary, :"$1"}], [:"$1"]}]

          flush_internal_key_pages(
            shard_index,
            keydir,
            :ets.select(keydir, match_spec, @internal_delete_batch_size)
          )
        after
          :ets.safe_fixtable(keydir, false)
        end

      case result do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  rescue
    error in ArgumentError -> {:error, {:flush_internal_keydir_unavailable, error}}
  end

  defp flush_internal_key_pages(_shard_index, _keydir, :"$end_of_table"), do: :ok

  defp flush_internal_key_pages(shard_index, keydir, {keys, continuation}) do
    batch =
      keys
      |> Enum.filter(&InternalKey.reserved?/1)
      |> Enum.reject(&backfill_metadata_key?(shard_index, &1))

    with :ok <- flush_internal_key_batch(shard_index, batch) do
      flush_internal_key_pages(shard_index, keydir, :ets.select(continuation))
    end
  end

  defp flush_internal_key_batch(_shard_index, []), do: :ok

  defp flush_internal_key_batch(shard_index, batch) do
    case Ferricstore.Raft.Backend.write_delete_batch(shard_index, batch) do
      {:ok, results} when is_list(results) ->
        if length(results) == length(batch) and Enum.all?(results, &(&1 == :ok)) do
          :ok
        else
          {:error, {:flush_internal_keys_failed, shard_index, results}}
        end

      {:error, _reason} = error ->
        error

      other ->
        {:error, {:flush_internal_keys_failed, shard_index, other}}
    end
  end

  defp finalize_empty_flow_shards(ctx) do
    0..max(ctx.shard_count - 1, -1)//1
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

  defp backfill_metadata_key?(%{shard_count: shard_count}, key) do
    Enum.any?(0..max(shard_count - 1, -1)//1, &backfill_metadata_key?(&1, key))
  end

  defp backfill_metadata_key?(shard_index, key) do
    key == Keys.shared_value_ref_backfill_key(shard_index) or
      key == SharedRefBackfill.progress_key(shard_index)
  end

  defp flush_key(store, key) do
    type_key = CompoundKey.type_key(key)

    case CompoundOps.compound_get(store, key, type_key) do
      "hash" ->
        run_flush_steps(key, [
          fn -> CompoundOps.compound_delete_prefix(store, key, CompoundKey.hash_prefix(key)) end,
          fn -> CompoundOps.compound_delete(store, key, type_key) end
        ])

      "list" ->
        run_flush_steps(key, [
          fn -> CompoundOps.compound_delete_prefix(store, key, CompoundKey.list_prefix(key)) end,
          fn -> CompoundOps.compound_delete(store, key, CompoundKey.list_meta_key(key)) end,
          fn -> CompoundOps.compound_delete(store, key, type_key) end,
          fn -> DeleteOps.delete(store, key) end
        ])

      "set" ->
        run_flush_steps(key, [
          fn -> CompoundOps.compound_delete_prefix(store, key, CompoundKey.set_prefix(key)) end,
          fn -> CompoundOps.compound_delete(store, key, type_key) end
        ])

      "zset" ->
        run_flush_steps(key, [
          fn -> CompoundOps.compound_delete_prefix(store, key, CompoundKey.zset_prefix(key)) end,
          fn -> CompoundOps.compound_delete(store, key, type_key) end
        ])

      "stream" ->
        run_flush_steps(key, [
          fn -> CompoundOps.compound_delete_prefix(store, key, "X:" <> key <> <<0>>) end,
          fn ->
            CompoundOps.compound_delete_prefix(store, key, CompoundKey.stream_group_prefix(key))
          end,
          fn -> CompoundOps.compound_delete(store, key, CompoundKey.stream_meta_key(key)) end,
          fn -> CompoundOps.compound_delete(store, key, type_key) end,
          fn -> DeleteOps.delete(store, key) end
        ])

      nil ->
        DeleteOps.delete(store, key)

      _unknown ->
        run_flush_steps(key, [
          fn -> CompoundOps.compound_delete(store, key, type_key) end,
          fn -> DeleteOps.delete(store, key) end
        ])
    end
  end

  defp run_flush_steps(key, steps) do
    Enum.reduce_while(steps, :ok, fn step, :ok ->
      case step.() do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
        other -> {:halt, {:error, {:flush_key_failed, key, other}}}
      end
    end)
  end

  defp clear_stream_tables do
    Ferricstore.Stream.LocalState.clear()
  end
end
