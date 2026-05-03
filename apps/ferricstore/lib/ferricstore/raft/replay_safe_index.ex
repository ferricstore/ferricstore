defmodule Ferricstore.Raft.ReplaySafeIndex do
  @moduledoc false

  require Logger

  alias Ferricstore.Bitcask.NIF

  @filename "raft_replay_safe.index"

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

  @spec persist(binary(), non_neg_integer()) :: :ok
  def persist(shard_data_path, index) when is_integer(index) and index >= 0 do
    File.mkdir_p!(shard_data_path)

    marker_path = path(shard_data_path)
    tmp_path = marker_path <> ".tmp"
    contents = Integer.to_string(index) <> "\n"

    result =
      with :ok <- File.write(tmp_path, contents),
           :ok <- warn_fsync(NIF.v2_fsync(tmp_path), tmp_path),
           :ok <- Ferricstore.FS.rename(tmp_path, marker_path),
           :ok <- warn_fsync(NIF.v2_fsync_dir(shard_data_path), shard_data_path) do
        :ok
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        _ = Ferricstore.FS.rm(tmp_path)
        Logger.warning("failed to persist raft replay-safe index #{index}: #{inspect(reason)}")
        :ok
    end
  end

  defp warn_fsync(:ok, _path), do: :ok

  defp warn_fsync({:error, reason}, path) do
    Logger.warning("failed to fsync raft replay-safe index path #{path}: #{inspect(reason)}")
    :ok
  end
end
