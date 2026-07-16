defmodule Ferricstore.Store.ActiveFile do
  @moduledoc """
  Tracks the active log file for each shard.

  Uses a per-row atomics generation counter plus a process dictionary cache.
  Reads stay one `Process.get/1` plus one `:atomics.get/2` on the hot path,
  while a rotation invalidates only callers that cached that exact shard.

  ## Usage

      # In Shard init and rotation:
      ActiveFile.publish(instance_ctx, shard_index, file_id, file_path, shard_data_path)

      # In Router's local-origin write path (hot path):
      {file_id, file_path, shard_data_path} = ActiveFile.get(instance_ctx, shard_index)
  """

  @table :ferricstore_active_files

  @doc """
  Initializes the registry. Called once from Application.start.
  """
  @spec init(non_neg_integer()) :: :ok
  def init(_shard_count) do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
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

  Every instance uses `{instance_name, shard_index}` so registry identity is
  uniform across default and embedded stores.
  """
  @spec publish(map() | nil, non_neg_integer(), non_neg_integer(), binary(), binary()) :: :ok
  def publish(ctx, shard_index, file_id, file_path, shard_data_path) do
    table_key = table_key(ctx, shard_index)
    publish_row(table_key, file_id, file_path, shard_data_path)
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
    table_key = table_key(ctx, shard_index)
    cache_key = {:active_file_cache, table_key}

    case Process.get(cache_key) do
      {ref, cached_gen, file_id, file_path, shard_data_path} ->
        if :atomics.get(ref, 1) == cached_gen do
          {file_id, file_path, shard_data_path}
        else
          refresh_cache(table_key, cache_key)
        end

      _ ->
        refresh_cache(table_key, cache_key)
    end
  end

  @doc false
  @spec delete(non_neg_integer()) :: :ok
  def delete(shard_index), do: delete(nil, shard_index)

  @doc false
  @spec delete(map() | nil, non_neg_integer()) :: :ok
  def delete(ctx, shard_index) do
    table_key = table_key(ctx, shard_index)
    delete_row(table_key)
    Process.delete({:active_file_cache, table_key})
    :ok
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
        table_key = {name, shard_index}
        delete_row(table_key)
        Process.delete({:active_file_cache, table_key})
      end)
    end

    :ok
  end

  defp table_key(nil, shard_index), do: {:default, shard_index}
  defp table_key(%{name: :default}, shard_index), do: {:default, shard_index}
  defp table_key(%{name: name}, shard_index), do: {name, shard_index}

  defp publish_row(table_key, file_id, file_path, shard_data_path) do
    case :ets.lookup(@table, table_key) do
      [{^table_key, ref, _old_file_id, _old_file_path, _old_shard_path}] ->
        :ets.insert(@table, {table_key, ref, file_id, file_path, shard_data_path})
        :atomics.add(ref, 1, 1)
        :ok

      [] ->
        ref = :atomics.new(1, signed: false)

        if :ets.insert_new(@table, {table_key, ref, file_id, file_path, shard_data_path}) do
          :ok
        else
          publish_row(table_key, file_id, file_path, shard_data_path)
        end
    end
  end

  defp refresh_cache(table_key, cache_key) do
    case :ets.lookup(@table, table_key) do
      [{^table_key, ref, _file_id, _file_path, _shard_data_path}] ->
        generation_before = :atomics.get(ref, 1)

        case :ets.lookup(@table, table_key) do
          [{^table_key, ^ref, file_id, file_path, shard_data_path}] ->
            if :atomics.get(ref, 1) == generation_before do
              Process.put(
                cache_key,
                {ref, generation_before, file_id, file_path, shard_data_path}
              )

              {file_id, file_path, shard_data_path}
            else
              refresh_cache(table_key, cache_key)
            end

          _changed_or_deleted ->
            refresh_cache(table_key, cache_key)
        end

      [] ->
        Process.delete(cache_key)
        raise MatchError, term: []
    end
  end

  defp delete_row(table_key) do
    case :ets.take(@table, table_key) do
      [{^table_key, ref, _file_id, _file_path, _shard_data_path}] ->
        :atomics.add(ref, 1, 1)
        :ok

      [] ->
        :ok
    end
  end
end
