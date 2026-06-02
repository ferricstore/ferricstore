defmodule Ferricstore.Flow.HistoryProjectedIndex do
  @moduledoc false

  require Logger

  alias Ferricstore.Bitcask.NIF

  @filename "flow_history_projected.index"

  @spec path(binary()) :: binary()
  def path(shard_data_path), do: Path.join(shard_data_path, @filename)

  @spec read(binary()) :: non_neg_integer()
  def read(shard_data_path) do
    shard_data_path
    |> path()
    |> File.read()
    |> case do
      {:ok, bin} ->
        bin
        |> String.trim()
        |> Integer.parse()
        |> case do
          {idx, ""} when idx >= 0 -> idx
          _ -> 0
        end

      _ ->
        0
    end
  end

  @spec persist(binary(), non_neg_integer()) :: :ok | {:error, term()}
  def persist(shard_data_path, index) when is_integer(index) and index >= 0 do
    marker_path = path(shard_data_path)

    with_marker_lock(marker_path, fn ->
      persist_locked(shard_data_path, marker_path, index)
    end)
  end

  defp persist_locked(shard_data_path, marker_path, index) do
    current_index = read(shard_data_path)
    target_index = max(index, current_index)

    if target_index == current_index and File.regular?(marker_path) do
      :ok
    else
      write_locked(shard_data_path, marker_path, target_index)
    end
  end

  defp write_locked(shard_data_path, marker_path, target_index) do
    tmp_path = unique_tmp_path(marker_path)
    contents = Integer.to_string(target_index) <> "\n"

    result =
      with :ok <- Ferricstore.FS.mkdir_p(shard_data_path),
           :ok <- File.write(tmp_path, contents),
           :ok <- fsync(NIF.v2_fsync(tmp_path), tmp_path),
           :ok <- Ferricstore.FS.rename(tmp_path, marker_path),
           :ok <- fsync(NIF.v2_fsync_dir(shard_data_path), shard_data_path) do
        :ok
      end

    case result do
      :ok ->
        :ok

      {:error, _reason} = error ->
        cleanup_tmp(tmp_path)

        Logger.warning(
          "failed to persist Flow history projected index #{target_index}: #{inspect(error)}"
        )

        error
    end
  end

  defp with_marker_lock(marker_path, fun) when is_function(fun, 0) do
    # Marker advancement is cold-path durability metadata. Serialize by path so
    # concurrent flushers cannot rename over each other's tmp file or publish a
    # lower watermark after a higher one.
    lock = {{__MODULE__, :marker, marker_path}, self()}

    case :global.trans(lock, fun, [node()]) do
      :aborted -> {:error, :history_projected_index_lock_busy}
      result -> result
    end
  end

  defp unique_tmp_path(marker_path) do
    marker_path <> ".tmp." <> Integer.to_string(:erlang.unique_integer([:positive, :monotonic]))
  end

  defp cleanup_tmp(tmp_path) do
    case Ferricstore.FS.rm(tmp_path) do
      :ok ->
        :ok

      {:error, {:not_found, _}} ->
        :ok

      {:error, reason} ->
        :telemetry.execute(
          [:ferricstore, :flow, :history_projected_index, :cleanup_failed],
          %{count: 1},
          %{path: tmp_path, reason: reason}
        )

        Logger.warning(
          "failed to remove Flow history projected tmp index #{tmp_path}: #{inspect(reason)}"
        )
    end
  end

  defp fsync(:ok, _path), do: :ok

  defp fsync({:error, reason}, path) do
    Logger.warning(
      "failed to fsync Flow history projected index path #{path}: #{inspect(reason)}"
    )

    {:error, {:fsync_failed, path, reason}}
  end
end
