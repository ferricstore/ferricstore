defmodule Ferricstore.Store.BlobStore do
  @moduledoc """
  Content-addressed side-channel blob storage for large values.

  Large values are stored once under `data_dir/blob/shard_N/`, while Bitcask
  stores the fixed-size `BlobRef`. Raft still receives the logical command
  payload today; replacing that with ref-only replication needs a separate blob
  transfer protocol so followers can prove the blob exists before apply.
  """

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Store.BlobRef

  @hash_chunk_bytes 1_048_576
  @tmp_stale_after_seconds 300

  @type reason :: term()

  @doc """
  Stores `payload` under its content-addressed path and returns the small ref.

  If the same complete blob already exists, this returns without rewriting it.
  That keeps fanout-style workloads from writing identical large payload bytes
  once per workflow/key.
  """
  @spec put(binary(), non_neg_integer(), binary()) :: {:ok, BlobRef.t()} | {:error, reason()}
  def put(data_dir, shard_index, payload)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
             is_binary(payload) do
    ref = BlobRef.from_payload(payload)
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      case existing_complete?(path, ref) do
        true -> {:ok, ref}
        false -> write_atomic(path, payload, ref)
        {:error, reason} -> {:error, reason}
      end

    case result do
      {:ok, ^ref} = ok ->
        ok

      {:error, reason} = error ->
        emit_error(:put, shard_index, path, ref, reason)
        error
    end
  end

  @doc "Reads and validates a blob by ref."
  @spec get(binary(), non_neg_integer(), BlobRef.t()) :: {:ok, binary()} | {:error, reason()}
  def get(data_dir, shard_index, %BlobRef{} = ref)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      with {:ok, payload} <- File.read(path),
           :ok <- verify_size(ref, payload),
           :ok <- verify_checksum(ref, payload) do
        {:ok, payload}
      end

    case result do
      {:ok, _payload} = ok ->
        ok

      {:error, reason} = error ->
        emit_error(:get, shard_index, path, ref, reason)
        error
    end
  end

  @doc """
  Verifies that an existing blob file exactly matches its content-addressed ref.

  This is intended for write/apply correctness boundaries where a ref-only
  command would otherwise acknowledge a pointer without proving the pointed
  bytes are intact. It hashes the file in chunks and does not materialize the
  full payload as a BEAM binary.
  """
  @spec verify(binary(), non_neg_integer(), BlobRef.t()) :: :ok | {:error, reason()}
  def verify(data_dir, shard_index, %BlobRef{size: size} = ref)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      with :ok <- stat_regular_size(path, size),
           :ok <- file_matches_ref?(path, ref) do
        :ok
      end

    case result do
      :ok ->
        :ok

      :mismatch ->
        error = {:error, :checksum_mismatch}
        emit_error(:verify, shard_index, path, ref, :checksum_mismatch)
        error

      {:error, reason} = error ->
        emit_error(:verify, shard_index, path, ref, reason)
        error
    end
  end

  @doc """
  Returns a file ref for a blob after validating the file is regular and has
  the expected size.

  This is the hot streaming path. It intentionally does not hash the blob on
  every read; `put/3` verifies existing blobs before dedupe, and `get/3` still
  verifies materialized reads. Full checksum validation belongs in write-time
  validation and background scrub, not in every sendfile/file-stream GET.
  """
  @spec file_ref(binary(), non_neg_integer(), BlobRef.t()) ::
          {:ok, {binary(), 0, non_neg_integer()}} | {:error, reason()}
  def file_ref(data_dir, shard_index, %BlobRef{size: size} = ref)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    path = BlobRef.path(data_dir, shard_index, ref)

    result =
      with :ok <- stat_regular_size(path, size) do
        {:ok, {path, 0, size}}
      end

    case result do
      {:ok, _file_ref} = ok ->
        ok

      {:error, reason} = error ->
        emit_error(:file_ref, shard_index, path, ref, reason)
        error
    end
  end

  @doc """
  Deletes blob files that are not present in `live_refs`.

  The caller owns producing a complete live set. This function is deliberately
  dumb: it only compares content-addressed paths and removes files outside that
  set. That keeps the correctness boundary in the shard, which knows current
  shared and promoted/dedicated Bitcask locations.
  """
  @spec sweep_unreferenced(binary(), non_neg_integer(), Enumerable.t()) ::
          {:ok,
           %{
             deleted_files: non_neg_integer(),
             deleted_bytes: non_neg_integer(),
             kept_files: non_neg_integer()
           }}
          | {:error, term()}
  def sweep_unreferenced(data_dir, shard_index, live_refs)
      when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    shard_path = Ferricstore.DataDir.blob_shard_path(data_dir, shard_index)
    live_paths = live_relative_paths(live_refs)

    case blob_files(shard_path) do
      {:ok, paths} ->
        sweep_blob_paths(shard_path, paths, live_paths)

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Returns current blob side-channel storage usage for metrics and diagnostics.

  This scans the blob directory, so callers should use it for observability
  paths only, not per-command hot paths.
  """
  @spec storage_stats(binary()) ::
          {:ok,
           %{
             files: non_neg_integer(),
             bytes: non_neg_integer(),
             tmp_files: non_neg_integer(),
             tmp_bytes: non_neg_integer()
           }}
          | {:error, term()}
  def storage_stats(data_dir) when is_binary(data_dir) do
    blob_glob = Path.join([data_dir, "blob", "shard_*", "**", "*.blob"])
    tmp_glob = Path.join([data_dir, "blob", "shard_*", "**", "*.tmp"])

    with {:ok, blob_stats} <- storage_stats_for_paths(Path.wildcard(blob_glob)),
         {:ok, tmp_stats} <- storage_stats_for_paths(Path.wildcard(tmp_glob, match_dot: true)) do
      {:ok,
       %{
         files: blob_stats.files,
         bytes: blob_stats.bytes,
         tmp_files: tmp_stats.files,
         tmp_bytes: tmp_stats.bytes
       }}
    end
  rescue
    error -> {:error, {:blob_storage_stats_failed, error}}
  end

  defp storage_stats_for_paths(paths) do
    Enum.reduce_while(paths, {:ok, %{files: 0, bytes: 0}}, fn path, {:ok, acc} ->
      case File.stat(path) do
        {:ok, %{type: :regular, size: size}} ->
          {:cont, {:ok, %{files: acc.files + 1, bytes: acc.bytes + size}}}

        {:ok, %{type: type}} ->
          {:halt, {:error, {:blob_storage_stats_invalid_file, path, type}}}

        {:error, reason} ->
          {:halt, {:error, {:blob_storage_stats_stat_failed, path, reason}}}
      end
    end)
  end

  defp existing_complete?(path, %BlobRef{size: expected_size} = ref) do
    case stat_regular_size(path, expected_size) do
      :ok ->
        case file_matches_ref?(path, ref) do
          :ok -> true
          :mismatch -> false
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} when reason in [:invalid_blob_file, :size_mismatch] ->
        false

      {:error, :enoent} ->
        false

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp stat_regular_size(path, expected_size) do
    case File.stat(path) do
      {:ok, %{type: :regular, size: ^expected_size}} -> :ok
      {:ok, %{type: :regular}} -> {:error, :size_mismatch}
      {:ok, _other} -> {:error, :invalid_blob_file}
      {:error, reason} -> {:error, reason}
    end
  end

  defp write_atomic(path, payload, ref) do
    dir = Path.dirname(path)
    tmp_path = tmp_path(path)
    dir_existed? = Ferricstore.FS.dir?(dir)

    result =
      with :ok <- Ferricstore.FS.mkdir_p(dir),
           :ok <- fsync_parent_after_mkdir(dir, dir_existed?),
           :ok <- File.write(tmp_path, payload, [:binary]),
           :ok <- fsync_file(tmp_path),
           :ok <- Ferricstore.FS.rename(tmp_path, path),
           :ok <- fsync_dir(dir) do
        {:ok, ref}
      end

    case result do
      {:ok, ^ref} = ok ->
        ok

      {:error, _reason} = error ->
        cleanup_tmp(tmp_path)
        error
    end
  end

  defp tmp_path(path) do
    basename = Path.basename(path)
    suffix = System.unique_integer([:positive, :monotonic])
    Path.join(Path.dirname(path), ".#{basename}.#{suffix}.tmp")
  end

  defp fsync_parent_after_mkdir(_dir, true), do: :ok

  defp fsync_parent_after_mkdir(dir, false) do
    # The first blob in a checksum-prefix directory must make the directory
    # entry durable. Later blobs in the same prefix only need the final fsync
    # on `dir` after their atomic rename.
    fsync_dir(Path.dirname(dir))
  end

  defp fsync_file(path), do: normalize_fsync(NIF.v2_fsync(path))

  defp fsync_dir(path) do
    case Process.get(:ferricstore_blob_store_fsync_dir_hook) do
      fun when is_function(fun, 1) -> normalize_fsync(fun.(path))
      _ -> normalize_fsync(NIF.v2_fsync_dir(path))
    end
  end

  defp normalize_fsync(:ok), do: :ok
  defp normalize_fsync({:error, reason}), do: {:error, reason}

  defp emit_error(operation, shard_index, path, %BlobRef{size: size}, reason) do
    :telemetry.execute(
      [:ferricstore, :blob, :error],
      %{count: 1, bytes: size},
      %{operation: operation, shard_index: shard_index, reason: reason, path: path}
    )
  end

  defp file_matches_ref?(path, %BlobRef{checksum: expected_checksum}) do
    case File.open(path, [:read, :raw, :binary]) do
      {:ok, io} ->
        try do
          case hash_file(io, :crypto.hash_init(:sha256)) do
            {:ok, ^expected_checksum} -> :ok
            {:ok, _other_checksum} -> :mismatch
            {:error, reason} -> {:error, reason}
          end
        after
          :file.close(io)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp hash_file(io, hash_state) do
    case :file.read(io, @hash_chunk_bytes) do
      {:ok, chunk} when is_binary(chunk) and byte_size(chunk) > 0 ->
        hash_file(io, :crypto.hash_update(hash_state, chunk))

      :eof ->
        {:ok, :crypto.hash_final(hash_state)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp verify_size(%BlobRef{size: size}, payload) do
    if byte_size(payload) == size do
      :ok
    else
      {:error, :size_mismatch}
    end
  end

  defp verify_checksum(%BlobRef{} = ref, payload) do
    if BlobRef.verify_payload?(ref, payload) do
      :ok
    else
      {:error, :checksum_mismatch}
    end
  end

  defp cleanup_tmp(tmp_path) do
    case Ferricstore.FS.rm(tmp_path) do
      :ok -> :ok
      {:error, {:not_found, _message}} -> :ok
      {:error, _reason} -> :ok
    end
  end

  defp live_relative_paths(live_refs) do
    Enum.reduce(live_refs, MapSet.new(), fn
      %BlobRef{} = ref, acc -> MapSet.put(acc, BlobRef.relative_path(ref))
      _other, acc -> acc
    end)
  end

  defp blob_files(shard_path) do
    if Ferricstore.FS.dir?(shard_path) do
      {:ok, Path.wildcard(Path.join(shard_path, "**/*.blob"))}
    else
      {:ok, []}
    end
  rescue
    error -> {:error, {:blob_list_failed, error}}
  end

  defp blob_tmp_files(shard_path) do
    if Ferricstore.FS.dir?(shard_path) do
      {:ok, Path.wildcard(Path.join(shard_path, "**/*.tmp"), match_dot: true)}
    else
      {:ok, []}
    end
  rescue
    error -> {:error, {:blob_tmp_list_failed, error}}
  end

  defp sweep_blob_paths(shard_path, paths, live_paths) do
    result =
      Enum.reduce_while(
        paths,
        {:ok, %{deleted_files: 0, deleted_bytes: 0, kept_files: 0}, MapSet.new()},
        fn path, {:ok, stats, dirs} ->
          relative = Path.relative_to(path, shard_path)

          if MapSet.member?(live_paths, relative) do
            {:cont, {:ok, %{stats | kept_files: stats.kept_files + 1}, dirs}}
          else
            case delete_blob_file(path) do
              {:ok, size} ->
                stats = %{
                  stats
                  | deleted_files: stats.deleted_files + 1,
                    deleted_bytes: stats.deleted_bytes + size
                }

                {:cont, {:ok, stats, MapSet.put(dirs, Path.dirname(path))}}

              {:error, _reason} = error ->
                {:halt, error}
            end
          end
        end
      )

    case result do
      {:ok, stats, dirs} ->
        with {:ok, tmp_stats, tmp_dirs} <- sweep_tmp_paths(shard_path),
             :ok <- fsync_deleted_dirs(MapSet.union(dirs, tmp_dirs)) do
          {:ok, Map.merge(stats, tmp_stats)}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp sweep_tmp_paths(shard_path) do
    case blob_tmp_files(shard_path) do
      {:ok, paths} ->
        Enum.reduce_while(
          paths,
          {:ok, %{deleted_tmp_files: 0, deleted_tmp_bytes: 0}, MapSet.new()},
          fn path, {:ok, stats, dirs} ->
            if stale_tmp_file?(path) do
              case delete_blob_file(path) do
                {:ok, size} ->
                  stats = %{
                    stats
                    | deleted_tmp_files: stats.deleted_tmp_files + 1,
                      deleted_tmp_bytes: stats.deleted_tmp_bytes + size
                  }

                  {:cont, {:ok, stats, MapSet.put(dirs, Path.dirname(path))}}

                {:error, _reason} = error ->
                  {:halt, error}
              end
            else
              {:cont, {:ok, stats, dirs}}
            end
          end
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp stale_tmp_file?(path) do
    case File.stat(path, time: :posix) do
      {:ok, %{type: :regular, mtime: mtime}} when is_integer(mtime) ->
        System.system_time(:second) - mtime >= @tmp_stale_after_seconds

      _ ->
        false
    end
  end

  defp delete_blob_file(path) do
    size =
      case File.stat(path) do
        {:ok, %{type: :regular, size: size}} -> size
        _ -> 0
      end

    case Ferricstore.FS.rm(path) do
      :ok -> {:ok, size}
      {:error, {:not_found, _message}} -> {:ok, 0}
      {:error, reason} -> {:error, {:blob_delete_failed, path, reason}}
    end
  end

  defp fsync_deleted_dirs(dirs) do
    Enum.reduce_while(dirs, :ok, fn dir, :ok ->
      case fsync_dir(dir) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:blob_delete_fsync_failed, dir, reason}}}
      end
    end)
  end
end
