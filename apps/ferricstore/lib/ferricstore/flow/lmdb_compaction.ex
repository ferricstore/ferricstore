defmodule Ferricstore.Flow.LMDBCompaction do
  @moduledoc false

  # LMDB reuses freed pages but never shrinks its data file, so after a large
  # retention cleanup a shard keeps its peak size on disk. At startup, before
  # any process opens a shard's environment, a mostly-free file is replaced by
  # a compacted copy. The copy is fsynced before it atomically replaces the
  # original, so a crash leaves either the old or the new complete file.

  require Logger

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.LMDB

  @data_file "data.mdb"
  @compact_file "data.mdb.compact"
  @default_min_free_ratio 0.5
  @default_min_file_bytes 256 * 1024 * 1024

  @spec compact_shards_at_startup(binary(), non_neg_integer()) :: :ok
  def compact_shards_at_startup(data_dir, shard_count)
      when is_binary(data_dir) and is_integer(shard_count) and shard_count >= 0 do
    if Application.get_env(:ferricstore, :flow_lmdb_startup_compaction, true) do
      opts = [
        min_free_ratio:
          Application.get_env(
            :ferricstore,
            :flow_lmdb_compaction_min_free_ratio,
            @default_min_free_ratio
          ),
        min_file_bytes:
          Application.get_env(
            :ferricstore,
            :flow_lmdb_compaction_min_file_bytes,
            @default_min_file_bytes
          )
      ]

      for shard_index <- 0..(shard_count - 1)//1 do
        path = data_dir |> Ferricstore.DataDir.shard_data_path(shard_index) |> LMDB.path()
        log_result(shard_index, maybe_compact(path, opts))
      end
    end

    :ok
  end

  @spec maybe_compact(binary(), keyword()) ::
          {:ok, :skipped | %{before_bytes: non_neg_integer(), after_bytes: non_neg_integer()}}
          | {:error, term()}
  def maybe_compact(path, opts) when is_binary(path) and is_list(opts) do
    data_file = Path.join(path, @data_file)
    compact_file = Path.join(path, @compact_file)
    min_free_ratio = Keyword.get(opts, :min_free_ratio, @default_min_free_ratio)
    min_file_bytes = Keyword.get(opts, :min_file_bytes, @default_min_file_bytes)
    _ = Ferricstore.FS.rm(compact_file)

    with {:ok, %File.Stat{type: :regular, size: before_bytes}} <- File.stat(data_file),
         :ok <- threshold(before_bytes >= min_file_bytes),
         {:ok, {file_pages, used_pages, _page_size}} <- NIF.lmdb_space(path, LMDB.map_size()),
         :ok <- threshold((file_pages - used_pages) / max(file_pages, 1) >= min_free_ratio),
         {:ok, _copied_bytes} <- NIF.lmdb_compact_copy(path, compact_file, LMDB.map_size()),
         :ok <- LMDB.release(path),
         :ok <- Ferricstore.FS.rename(compact_file, data_file),
         :ok <- NIF.v2_fsync_dir(path),
         {:ok, %File.Stat{size: after_bytes}} <- File.stat(data_file) do
      {:ok, %{before_bytes: before_bytes, after_bytes: after_bytes}}
    else
      :skip ->
        {:ok, :skipped}

      {:error, :enoent} ->
        {:ok, :skipped}

      {:error, _reason} = error ->
        _ = Ferricstore.FS.rm(compact_file)
        error

      other ->
        _ = Ferricstore.FS.rm(compact_file)
        {:error, {:unexpected_lmdb_compaction_result, other}}
    end
  end

  defp threshold(true), do: :ok
  defp threshold(false), do: :skip

  defp log_result(_shard_index, {:ok, :skipped}), do: :ok

  defp log_result(shard_index, {:ok, %{before_bytes: before_bytes, after_bytes: after_bytes}}) do
    Logger.info(
      "Flow LMDB shard #{shard_index} compacted at startup: #{before_bytes} -> #{after_bytes} bytes"
    )
  end

  defp log_result(shard_index, {:error, reason}) do
    Logger.warning(
      "Flow LMDB shard #{shard_index} startup compaction skipped: #{inspect(reason, limit: 20)}"
    )
  end
end
