defmodule Ferricstore.ProbCleanup do
  @moduledoc false

  @spec flush_all(binary(), pos_integer()) :: :ok | {:error, term()}
  def flush_all(data_dir, shard_count)
      when is_binary(data_dir) and is_integer(shard_count) and shard_count > 0 do
    try do
      Enum.reduce_while(0..(shard_count - 1), :ok, fn i, :ok ->
        shard_path = Ferricstore.DataDir.shard_data_path(data_dir, i)
        prob_dir = Path.join(shard_path, "prob")

        case clear_dir(prob_dir) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    rescue
      exception ->
        emit_failure(:flush_prob_dirs, data_dir, exception, %{kind: :error})
        {:error, {:flush_prob_dirs_failed, :error, exception}}
    catch
      kind, reason ->
        emit_failure(:flush_prob_dirs, data_dir, reason, %{kind: kind})
        {:error, {:flush_prob_dirs_failed, kind, reason}}
    end
  end

  def flush_all(_data_dir, _shard_count), do: {:error, :invalid_shard_count}

  @spec clear_dir(binary()) :: :ok | {:error, term()}
  def clear_dir(prob_dir) do
    if Ferricstore.FS.exists?(prob_dir) do
      case Ferricstore.FS.ls(prob_dir) do
        {:ok, []} ->
          :ok

        {:ok, files} ->
          with :ok <- delete_files(prob_dir, files),
               :ok <- fsync_dir(prob_dir, :flush_prob_dir) do
            :ok
          end

        {:error, {:not_found, _message}} ->
          :ok

        {:error, reason} ->
          emit_failure(:list_prob_dir, prob_dir, reason)
          {:error, {:list_prob_dir_failed, prob_dir, reason}}
      end
    else
      :ok
    end
  end

  defp delete_files(prob_dir, files) do
    Enum.reduce_while(files, :ok, fn file, :ok ->
      path = Path.join(prob_dir, file)

      case Ferricstore.FS.rm(path) do
        :ok ->
          {:cont, :ok}

        {:error, reason} ->
          emit_failure(:delete_prob_file, path, reason)
          {:halt, {:error, {:delete_prob_file_failed, file, reason}}}
      end
    end)
  end

  defp fsync_dir(path, phase) do
    case fsync_dir_result(path) do
      :ok ->
        :ok

      {:error, reason} ->
        emit_failure(phase, path, reason)
        {:error, {:fsync_dir_failed, phase, reason}}
    end
  end

  defp fsync_dir_result(path) do
    case Process.get(:ferricstore_prob_command_fsync_dir_hook) do
      fun when is_function(fun, 1) -> fun.(path)
      _ -> Ferricstore.Bitcask.NIF.v2_fsync_dir(path)
    end
  end

  defp emit_failure(phase, path, reason, extra_metadata \\ %{}) do
    metadata =
      extra_metadata
      |> Map.put(:phase, phase)
      |> Map.put(:path, path)
      |> Map.put(:reason, reason)

    :telemetry.execute([:ferricstore, :prob_cleanup, :failed], %{count: 1}, metadata)
  end
end
