defmodule Ferricstore.Flow.LMDBReplaySafeIndex do
  @moduledoc false

  require Logger

  alias Ferricstore.Bitcask.NIF

  @filename "flow_lmdb_replay_safe.index"

  @spec path(binary()) :: binary()
  def path(shard_data_path), do: Path.join(shard_data_path, @filename)

  @spec read(binary()) :: non_neg_integer()
  def read(shard_data_path) do
    shard_data_path
    |> path()
    |> File.read()
    |> case do
      {:ok, contents} ->
        contents
        |> String.trim()
        |> Integer.parse()
        |> case do
          {index, ""} when index >= 0 -> index
          _ -> 0
        end

      _ ->
        0
    end
  end

  @spec persist(binary(), non_neg_integer()) :: :ok | {:error, term()}
  def persist(shard_data_path, index) when is_integer(index) and index >= 0 do
    marker_path = path(shard_data_path)
    tmp_path = marker_path <> ".tmp"
    contents = Integer.to_string(index) <> "\n"

    result =
      with :ok <- File.mkdir_p(shard_data_path),
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
          "failed to persist Flow LMDB replay-safe index #{index}: #{inspect(error)}"
        )

        error
    end
  end

  defp cleanup_tmp(tmp_path) do
    case Ferricstore.FS.rm(tmp_path) do
      :ok ->
        :ok

      {:error, {:not_found, _}} ->
        :ok

      {:error, reason} ->
        :telemetry.execute(
          [:ferricstore, :flow, :lmdb_replay_safe_index, :cleanup_failed],
          %{count: 1},
          %{path: tmp_path, reason: reason}
        )

        Logger.warning(
          "failed to remove Flow LMDB replay-safe tmp index #{tmp_path}: #{inspect(reason)}"
        )
    end
  end

  defp fsync(:ok, _path), do: :ok

  defp fsync({:error, reason}, path) do
    Logger.warning("failed to fsync Flow LMDB replay-safe index path #{path}: #{inspect(reason)}")
    {:error, {:fsync_failed, path, reason}}
  end
end
