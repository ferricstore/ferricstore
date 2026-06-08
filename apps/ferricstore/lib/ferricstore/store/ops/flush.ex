defmodule Ferricstore.Store.Ops.Flush do
  @moduledoc false

  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Ops.Compound, as: CompoundOps
  alias Ferricstore.Store.Ops.Delete, as: DeleteOps
  alias Ferricstore.Store.Router

  def flush(ctx) do
    with :ok <- flush_keys(ctx, Router.keys(ctx)),
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
           :ok <- clear_flow_history_dirs(ctx) do
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
    |> CompoundKey.user_visible_keys()
    |> Enum.reduce_while(:ok, fn key, :ok ->
      case flush_key(store, key) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
        other -> {:halt, {:error, {:flush_key_failed, key, other}}}
      end
    end)
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
