defmodule Ferricstore.Flow.LMDBMirror do
  @moduledoc false

  alias Ferricstore.Store.Router

  def require_healthy(ctx, index_key, partition_key) do
    if degraded?(ctx, index_key, partition_key) do
      {:error, "ERR flow LMDB mirror degraded"}
    else
      :ok
    end
  end

  def degraded?(ctx, index_key, partition_key) do
    ctx
    |> index_shards(index_key, partition_key)
    |> Enum.any?(&degraded_shard?(ctx, &1))
  end

  def index_shards(ctx, _index_key, nil) do
    if is_integer(ctx.shard_count) and ctx.shard_count > 0 do
      Enum.to_list(0..(ctx.shard_count - 1))
    else
      []
    end
  end

  def index_shards(ctx, index_key, _partition_key),
    do: [Router.shard_for(ctx, index_key)]

  def degraded_shard?(ctx, shard_index) do
    degraded_flag?(ctx, shard_index) or flush_in_progress_shard?(ctx, shard_index)
  end

  def degraded_flag?(ctx, shard_index) do
    flag_idx = shard_index + 1

    case Map.get(ctx, :flow_lmdb_mirror_degraded) do
      ref when is_reference(ref) ->
        flag_idx <= :atomics.info(ref).size and :atomics.get(ref, flag_idx) == 1

      _ ->
        false
    end
  rescue
    _ -> false
  end

  def shard_paths(data_dir, shard_count) do
    Enum.map(0..(shard_count - 1), fn shard_index ->
      data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()
    end)
  end

  def paths_for_index(ctx, _index_key, nil) do
    shard_paths(ctx.data_dir, ctx.shard_count)
  end

  def paths_for_index(ctx, index_key, partition_key) when is_binary(partition_key) do
    shard_index = Router.shard_for(ctx, index_key)

    [
      ctx.data_dir
      |> Ferricstore.DataDir.shard_data_path(shard_index)
      |> Ferricstore.Flow.LMDB.path()
    ]
  end

  defp flush_in_progress_shard?(%{data_dir: data_dir}, shard_index)
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> Ferricstore.Flow.LMDB.path()
    |> Ferricstore.Flow.LMDB.flush_in_progress?()
  rescue
    _ -> false
  end

  defp flush_in_progress_shard?(_ctx, _shard_index), do: false
end
