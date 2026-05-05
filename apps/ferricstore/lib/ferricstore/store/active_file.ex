defmodule Ferricstore.Store.ActiveFile do
  @moduledoc """
  Tracks the active log file for each shard.

  Uses atomics generation counter + process dictionary cache — ~15ns reads
  on the hot path. On file rotation, only an ETS re-read (~100ns) happens
  per caller process. No global GC from persistent_term.put, which matters
  when the host app has 50K+ LiveView/Channel processes.

  ## Usage

      # In Shard init and rotation:
      ActiveFile.publish(instance_ctx, shard_index, file_id, file_path, shard_data_path)

      # In Router's local-origin write path (hot path):
      {file_id, file_path, shard_data_path} = ActiveFile.get(instance_ctx, shard_index)
  """

  @table :ferricstore_active_files
  @atomics_key :ferricstore_active_file_gen

  @doc """
  Initializes the registry. Called once from Application.start.
  """
  @spec init(non_neg_integer()) :: :ok
  def init(_shard_count) do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    end

    unless :persistent_term.get(@atomics_key, nil) do
      ref = :atomics.new(1, signed: false)
      :persistent_term.put(@atomics_key, ref)
    end

    :ok
  end

  @doc """
  Publishes the active file metadata for a shard.

  Called from `Shard.init/1` and `Shard.maybe_rotate_file/1`.
  """
  @spec publish(non_neg_integer(), non_neg_integer(), binary(), binary()) :: :ok
  def publish(shard_index, file_id, file_path, shard_data_path) do
    publish(nil, shard_index, file_id, file_path, shard_data_path)
  end

  @doc """
  Publishes active file metadata for a shard in a specific instance.

  The default instance keeps the historical shard-index key for backward
  compatibility. Custom instances use `{instance_name, shard_index}` so
  isolated tests and embedded instances cannot overwrite each other.
  """
  @spec publish(map() | nil, non_neg_integer(), non_neg_integer(), binary(), binary()) :: :ok
  def publish(ctx, shard_index, file_id, file_path, shard_data_path) do
    table_key = table_key(ctx, shard_index)
    :ets.insert(@table, {table_key, file_id, file_path, shard_data_path})
    ref = :persistent_term.get(@atomics_key)
    :atomics.add(ref, 1, 1)
    :ok
  end

  @doc """
  Returns `{file_id, file_path, shard_data_path}` for the given shard.

  ~15ns hot path (atomics check + process dictionary cache hit).
  ~100ns cold (ETS lookup on generation mismatch).
  """
  @spec get(non_neg_integer()) :: {non_neg_integer(), binary(), binary()}
  def get(shard_index) do
    get(nil, shard_index)
  end

  @doc """
  Returns active file metadata for a shard in a specific instance.

  Prefer this in production paths that already have an Instance context.
  """
  @spec get(map() | nil, non_neg_integer()) :: {non_neg_integer(), binary(), binary()}
  def get(ctx, shard_index) do
    ref = :persistent_term.get(@atomics_key)
    current_gen = :atomics.get(ref, 1)
    table_key = table_key(ctx, shard_index)

    case Process.get({:active_file_cache, table_key}) do
      {^current_gen, file_id, file_path, shard_data_path} ->
        {file_id, file_path, shard_data_path}

      _ ->
        prune_process_cache_if_generation_changed(current_gen)

        [{^table_key, file_id, file_path, shard_data_path}] =
          :ets.lookup(@table, table_key)

        Process.put(
          {:active_file_cache, table_key},
          {current_gen, file_id, file_path, shard_data_path}
        )

        {file_id, file_path, shard_data_path}
    end
  end

  @doc """
  Removes active-file rows for a custom instance.

  Called during instance cleanup. The generation bump invalidates process
  dictionary caches that may still hold paths for the stopped instance.
  """
  @spec cleanup_instance(map()) :: :ok
  def cleanup_instance(%{name: :default}), do: :ok

  def cleanup_instance(%{name: name, shard_count: shard_count}) do
    if :ets.whereis(@table) != :undefined and shard_count > 0 do
      Enum.each(0..(shard_count - 1), fn shard_index ->
        :ets.delete(@table, {name, shard_index})
      end)

      bump_generation()
    end

    :ok
  end

  defp table_key(nil, shard_index), do: shard_index
  defp table_key(%{name: :default}, shard_index), do: shard_index
  defp table_key(%{name: name}, shard_index), do: {name, shard_index}

  defp prune_process_cache_if_generation_changed(current_gen) do
    case Process.get(:active_file_cache_generation) do
      ^current_gen ->
        :ok

      _ ->
        Process.get()
        |> Enum.each(fn
          {{:active_file_cache, _key} = key, _value} -> Process.delete(key)
          _ -> :ok
        end)

        Process.put(:active_file_cache_generation, current_gen)
        :ok
    end
  end

  defp bump_generation do
    case :persistent_term.get(@atomics_key, nil) do
      nil -> :ok
      ref -> :atomics.add(ref, 1, 1)
    end
  end
end
