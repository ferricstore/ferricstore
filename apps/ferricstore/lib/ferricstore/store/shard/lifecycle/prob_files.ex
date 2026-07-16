defmodule Ferricstore.Store.Shard.Lifecycle.ProbFiles do
  @moduledoc false

  alias Ferricstore.ProbFile

  require Logger

  @spec validate(binary(), non_neg_integer()) :: :ok | {:error, term()}
  def validate(shard_data_path, shard_index) do
    prob_dir = Path.join(shard_data_path, "prob")

    case Ferricstore.FS.ls(prob_dir) do
      {:ok, files} -> validate_files(prob_dir, files, shard_index)
      {:error, {:not_found, _message}} -> :ok
      {:error, reason} -> {:error, {:list_prob_dir_failed, prob_dir, reason}}
    end
  end

  defp validate_files(prob_dir, files, shard_index) do
    Enum.reduce_while(files, {false, :ok}, fn filename, {removed?, :ok} ->
      cond do
        ProbFile.valid_filename?(filename) ->
          case validate_regular_file(prob_dir, filename) do
            :ok -> {:cont, {removed?, :ok}}
            {:error, reason} -> {:halt, {removed?, {:error, reason}}}
          end

        ProbFile.staged_filename?(filename) ->
          case Ferricstore.FS.rm(Path.join(prob_dir, filename)) do
            :ok ->
              Logger.warning(
                "Shard #{shard_index}: removed incomplete probabilistic sidecar #{filename}"
              )

              {:cont, {true, :ok}}

            {:error, reason} ->
              {:halt, {removed?, {:error, {:remove_staged_prob_file_failed, filename, reason}}}}
          end

        true ->
          {:halt, {removed?, {:error, {:invalid_prob_filename, filename}}}}
      end
    end)
    |> finalize_validation(prob_dir)
  end

  defp validate_regular_file(prob_dir, filename) do
    case File.lstat(Path.join(prob_dir, filename)) do
      {:ok, %File.Stat{type: :regular}} ->
        :ok

      {:ok, %File.Stat{type: type}} ->
        {:error, {:invalid_prob_file_type, filename, type}}

      {:error, reason} ->
        {:error, {:stat_prob_file_failed, filename, reason}}
    end
  end

  defp finalize_validation({false, :ok}, _prob_dir), do: :ok

  defp finalize_validation({true, :ok}, prob_dir) do
    case Ferricstore.Bitcask.NIF.v2_fsync_dir(prob_dir) do
      :ok -> :ok
      {:error, reason} -> {:error, {:fsync_prob_dir_failed, prob_dir, reason}}
    end
  end

  defp finalize_validation({_removed?, {:error, _reason} = error}, _prob_dir), do: error
end
