defmodule Ferricstore.Raft.ReplaySafeIndex do
  @moduledoc false

  require Logger

  alias Ferricstore.Bitcask.NIF

  @filename "raft_replay_safe.index"
  @max_marker_bytes 64

  @spec path(binary()) :: binary()
  def path(shard_data_path), do: Path.join(shard_data_path, @filename)

  @spec read(binary()) :: non_neg_integer()
  def read(shard_data_path) do
    shard_data_path
    |> path()
    |> Ferricstore.FS.read_nofollow(@max_marker_bytes)
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
      with :ok <- ensure_marker_dir(shard_data_path) do
        with_marker_lock(marker_path, fn ->
          if read(shard_data_path) >= index do
            :ok
          else
            with :ok <- write_exclusive(tmp_path, contents),
                 :ok <- fsync(NIF.v2_fsync(tmp_path), tmp_path),
                 :ok <- Ferricstore.FS.rename(tmp_path, marker_path),
                 :ok <- fsync(NIF.v2_fsync_dir(shard_data_path), shard_data_path) do
              :ok
            end
          end
        end)
      end

    case result do
      :ok ->
        :ok

      {:error, _reason} = error ->
        case Ferricstore.FS.rm(tmp_path) do
          :ok ->
            :ok

          {:error, {:not_found, _}} ->
            :ok

          {:error, {:not_a_directory, _}} ->
            :ok

          {:error, cleanup_reason} ->
            :telemetry.execute(
              [:ferricstore, :raft, :replay_safe_index, :cleanup_failed],
              %{count: 1},
              %{path: tmp_path, reason: cleanup_reason}
            )

            Logger.warning(
              "failed to remove raft replay-safe tmp index #{tmp_path}: #{inspect(cleanup_reason)}"
            )
        end

        Logger.warning("failed to persist raft replay-safe index #{index}: #{inspect(error)}")
        error
    end
  end

  defp ensure_marker_dir(path) do
    case Ferricstore.FS.mkdir_p(path) do
      :ok ->
        :ok

      {:error, {:already_exists, _}} ->
        if Ferricstore.FS.dir?(path), do: :ok, else: {:error, :enotdir}

      {:error, {:not_a_directory, _}} ->
        {:error, :enotdir}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp write_exclusive(path, contents) do
    case File.open(path, [:write, :binary, :exclusive]) do
      {:ok, io} ->
        write_result = IO.binwrite(io, contents)
        close_result = File.close(io)

        case {write_result, close_result} do
          {:ok, :ok} -> :ok
          {{:error, reason}, _close_result} -> {:error, reason}
          {:ok, {:error, reason}} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp with_marker_lock(marker_path, fun) when is_function(fun, 0) do
    lock = {{__MODULE__, marker_path}, self()}

    case :global.trans(lock, fun, [node()]) do
      :aborted -> {:error, :marker_lock_busy}
      result -> result
    end
  end

  defp fsync(:ok, _path), do: :ok

  defp fsync({:error, reason}, path) do
    Logger.warning("failed to fsync raft replay-safe index path #{path}: #{inspect(reason)}")
    {:error, {:fsync_failed, path, reason}}
  end
end
