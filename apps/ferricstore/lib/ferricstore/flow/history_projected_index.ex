defmodule Ferricstore.Flow.HistoryProjectedIndex do
  @moduledoc false

  require Logger

  @filename "flow_history_projected.index"
  @max_marker_bytes 21
  @max_index 0xFFFFFFFFFFFFFFFF

  @spec path(binary()) :: binary()
  def path(shard_data_path), do: Path.join(shard_data_path, @filename)

  @spec read(binary()) :: non_neg_integer()
  def read(shard_data_path) do
    case read_result(shard_data_path) do
      {:ok, index} -> index
      {:error, _reason} -> 0
    end
  end

  @spec read_result(binary()) :: {:ok, non_neg_integer()} | {:error, term()}
  def read_result(shard_data_path) when is_binary(shard_data_path),
    do: read_marker_result(path(shard_data_path))

  def read_result(_shard_data_path), do: {:error, :invalid_history_projected_index_path}

  @spec persist(binary(), non_neg_integer()) :: :ok | {:error, term()}
  def persist(shard_data_path, index)
      when is_binary(shard_data_path) and is_integer(index) and index >= 0 and
             index <= @max_index do
    marker_path = path(shard_data_path)

    with_marker_lock(marker_path, fn ->
      persist_locked(shard_data_path, marker_path, index)
    end)
  end

  def persist(_shard_data_path, _index), do: {:error, :invalid_durable_index}

  defp persist_locked(shard_data_path, marker_path, index) do
    case read_marker_result(marker_path) do
      {:ok, current} when current >= index ->
        :ok

      {:ok, _current} ->
        write_locked(shard_data_path, marker_path, index)

      {:error, reason} ->
        if recoverable_marker_error?(reason) do
          write_locked(shard_data_path, marker_path, index)
        else
          {:error, {:marker_read_failed, reason}}
        end
    end
  end

  defp write_locked(shard_data_path, marker_path, target) do
    contents = Integer.to_string(target) <> "\n"

    result =
      with :ok <- Ferricstore.FS.mkdir_p(shard_data_path),
           :ok <-
             Ferricstore.FS.atomic_replace_nofollow(
               marker_path,
               contents,
               @max_marker_bytes
             ) do
        :ok
      end

    case result do
      :ok ->
        :ok

      {:error, _reason} = error ->
        Logger.warning(
          "failed to persist Flow history projected index #{target}: #{inspect(error)}"
        )

        error
    end
  end

  defp with_marker_lock(marker_path, fun) when is_function(fun, 0) do
    lock = {{__MODULE__, :marker, marker_path}, self()}

    case :global.trans(lock, fun, [node()]) do
      :aborted -> {:error, :history_projected_index_lock_busy}
      result -> result
    end
  catch
    :exit, reason -> {:error, {:history_projected_index_lock_failed, reason}}
  end

  defp read_marker_result(marker_path) do
    with {:ok, contents} <- Ferricstore.FS.read_nofollow(marker_path, @max_marker_bytes),
         {:ok, index} <- decode_marker(contents) do
      {:ok, index}
    end
  end

  defp decode_marker(contents) when is_binary(contents) do
    case :binary.split(contents, "\n") do
      [digits, ""] when digits != "" ->
        case Integer.parse(digits) do
          {index, ""} when index >= 0 and index <= @max_index ->
            if Integer.to_string(index) == digits,
              do: {:ok, index},
              else: {:error, :invalid_history_projected_index}

          _invalid ->
            {:error, :invalid_history_projected_index}
        end

      _invalid ->
        {:error, :invalid_history_projected_index}
    end
  end

  defp recoverable_marker_error?(:invalid_history_projected_index), do: true

  defp recoverable_marker_error?({kind, _reason})
       when kind in [:not_found, :symlink, :too_large],
       do: true

  defp recoverable_marker_error?(_reason), do: false
end
