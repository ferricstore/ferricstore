defmodule Ferricstore.Merge.Manifest do
  @moduledoc """
  Crash-safe manifest for in-progress merge operations.

  Before a merge starts, the scheduler writes a manifest file to the shard's
  data directory describing the merge plan (which file IDs are being merged,
  the target output file ID). If the node crashes mid-merge, the next startup
  detects the manifest and cleans up the partial merge output.

  ## Manifest file format

  The manifest is a binary term file written atomically (write to `.tmp` then
  rename). It contains an Erlang term with the merge plan.

  ## Recovery protocol

  On shard startup, if a manifest exists:

  1. Delete any partial compaction temp files.
  2. Leave the original input files intact (they are still valid).
  3. Delete the manifest file.
  4. The shard opens normally — the next merge cycle will retry.

  This is safe because the shard compaction path writes to `compact_*.log`
  temporary files and only renames them over existing non-active inputs after
  the copy succeeds. Recovery must not delete ordinary numbered log files that
  are newer than the manifest inputs, because those can be the shard's active
  file after a crash between manifest write and compaction start.
  """

  require Logger

  @manifest_filename "merge_manifest.bin"

  @type merge_plan :: %{
          shard_index: non_neg_integer(),
          input_file_ids: [non_neg_integer()],
          started_at: integer()
        }

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Writes a merge manifest to the shard's data directory.

  The manifest is written atomically: first to a `.tmp` file, then renamed
  to the final path. This ensures the manifest is either fully written or
  absent — never partially written.

  ## Parameters

    * `data_dir` -- path to the shard's data directory
    * `plan` -- merge plan map with `:shard_index`, `:input_file_ids`

  ## Returns

    * `:ok` on success
    * `{:error, reason}` if the file cannot be written
  """
  @spec write(Path.t(), merge_plan()) :: :ok | {:error, term()}
  def write(data_dir, plan) do
    manifest_path = manifest_path(data_dir)
    tmp_path = manifest_path <> ".tmp"

    term =
      Map.merge(plan, %{
        started_at: System.system_time(:millisecond),
        version: 1
      })

    binary = :erlang.term_to_binary(term)

    with :ok <- File.write(tmp_path, binary),
         :ok <- Ferricstore.FS.rename(tmp_path, manifest_path),
         :ok <- Ferricstore.Bitcask.NIF.v2_fsync_dir(Path.dirname(manifest_path)) do
      :ok
    else
      {:error, reason} = err ->
        # Clean up tmp file on failure.
        _ = Ferricstore.FS.rm(tmp_path)
        Logger.error("Failed to write merge manifest at #{manifest_path}: #{inspect(reason)}")
        err
    end
  end

  @doc """
  Reads the merge manifest from the shard's data directory.

  Returns `{:ok, plan}` if a manifest exists, or `:none` if no manifest is
  present (normal state — no interrupted merge).
  """
  @spec read(Path.t()) :: {:ok, merge_plan()} | :none | {:error, term()}
  def read(data_dir) do
    path = manifest_path(data_dir)

    if Ferricstore.FS.exists?(path) do
      case File.read(path) do
        {:ok, binary} ->
          try do
            {:ok, :erlang.binary_to_term(binary)}
          rescue
            ArgumentError -> {:error, :corrupt_manifest}
          end

        {:error, reason} ->
          {:error, reason}
      end
    else
      :none
    end
  end

  @doc """
  Removes the merge manifest file. Called after a merge completes successfully
  or after crash recovery cleanup.
  """
  @spec delete(Path.t()) :: :ok | {:error, term()}
  def delete(data_dir) do
    path = manifest_path(data_dir)

    case Ferricstore.FS.rm(path) do
      :ok ->
        # Fsync the directory so the manifest's removal is durable —
        # otherwise on a crash the stale manifest can re-appear and
        # trigger a spurious "interrupted merge" cleanup on next boot.
        _ = Ferricstore.Bitcask.NIF.v2_fsync_dir(data_dir)
        :ok

      {:error, {:not_found, _}} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Checks for and recovers from an interrupted merge on startup.

  If a manifest exists, this function:
  1. Logs a warning about the interrupted merge.
  2. Removes any partial output files that may have been created.
  3. Deletes the manifest.

  The original input files are left intact. The keydir will be rebuilt from
  them during normal startup, and the next merge cycle will re-merge them.

  ## Parameters

    * `data_dir` -- path to the shard's data directory
    * `shard_index` -- for logging purposes

  ## Returns

    * `:ok` if no manifest was found or recovery succeeded
    * `{:error, reason}` if cleanup fails
  """
  @spec recover_if_needed(Path.t(), non_neg_integer()) :: :ok | {:error, term()}
  def recover_if_needed(data_dir, shard_index) do
    case read(data_dir) do
      :none ->
        :ok

      {:ok, plan} ->
        Logger.warning(
          "Shard #{shard_index}: found interrupted merge manifest " <>
            "(input files: #{inspect(plan[:input_file_ids])}). " <>
            "Cleaning up partial output and re-opening normally."
        )

        # Remove only temp files created by the current shard compaction path.
        # Numbered log files greater than the input set may be legitimate active
        # files when the crash happens after manifest write but before copy.
        cleanup_partial_output(data_dir)
        delete(data_dir)

      {:error, :corrupt_manifest} ->
        Logger.warning("Shard #{shard_index}: found corrupt merge manifest. Deleting it.")

        delete(data_dir)

      {:error, reason} ->
        Logger.error("Shard #{shard_index}: failed to read merge manifest: #{inspect(reason)}")

        {:error, reason}
    end
  end

  @doc """
  Returns `true` if a merge manifest exists in the given data directory.
  """
  @spec exists?(Path.t()) :: boolean()
  def exists?(data_dir) do
    Ferricstore.FS.exists?(manifest_path(data_dir))
  end

  # -------------------------------------------------------------------
  # Private helpers
  # -------------------------------------------------------------------

  defp manifest_path(data_dir) do
    Path.join(data_dir, @manifest_filename)
  end

  # Remove only compaction temp files created by Shard.handle_call({:run_compaction, ...}).
  # The active file is often newer than the selected merge inputs, so deleting
  # all numbered files above max(input_file_ids) would drop valid writes.
  defp cleanup_partial_output(data_dir) do
    case Ferricstore.FS.ls(data_dir) do
      {:ok, files} ->
        files
        |> Enum.filter(&partial_output_file?/1)
        |> Enum.each(&remove_partial(&1, data_dir))

        # One dir fsync after the whole sweep so the removals are
        # durable. Without this a double-crash can resurrect stale
        # partial output files.
        _ = Ferricstore.Bitcask.NIF.v2_fsync_dir(data_dir)
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  defp partial_output_file?(name) do
    String.starts_with?(name, "compact_") and String.ends_with?(name, ".log")
  end

  defp remove_partial(name, data_dir) do
    Logger.info("Removing partial merge output: #{name}")
    _ = Ferricstore.FS.rm(Path.join(data_dir, name))
    :ok
  end
end
