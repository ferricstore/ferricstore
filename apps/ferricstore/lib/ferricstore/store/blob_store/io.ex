defmodule Ferricstore.Store.BlobStore.IO do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore.TableOwner

      defp segment_path(data_dir, shard_index, segment_id) do
        Path.join([
          Ferricstore.DataDir.blob_shard_path(data_dir, shard_index),
          "segments",
          BlobRef.segment_filename(segment_id)
        ])
      end

      defp stat_regular_size(path, expected_size) do
        case File.lstat(path) do
          {:ok, %{type: :regular, size: ^expected_size}} -> :ok
          {:ok, %{type: :regular}} -> {:error, :size_mismatch}
          {:ok, _other} -> {:error, :invalid_blob_file}
          {:error, reason} -> {:error, reason}
        end
      end

      defp stat_regular_min_size(path, min_size) do
        case File.lstat(path) do
          {:ok, %{type: :regular, size: size}} when size >= min_size -> :ok
          {:ok, %{type: :regular}} -> {:error, :size_mismatch}
          {:ok, _other} -> {:error, :invalid_blob_file}
          {:error, reason} -> {:error, reason}
        end
      end

      defp pread_exact_open(io, offset, size) do
        case :file.pread(io, offset, size) do
          {:ok, payload} when byte_size(payload) == size -> {:ok, payload}
          {:ok, _short} -> {:error, :size_mismatch}
          :eof -> {:error, :enoent}
          {:error, reason} -> {:error, reason}
        end
      end

      defp open_read_file(path) do
        modes = [:read, :raw, :binary]

        with :ok <- ensure_safe_blob_file_for_read(path) do
          case Process.get(:ferricstore_blob_store_open_read_hook) do
            fun when is_function(fun, 2) -> fun.(path, modes)
            _other -> File.open(path, modes)
          end
        end
      end

      defp ensure_safe_blob_file_for_read(path) do
        case File.lstat(path) do
          {:ok, %{type: :regular}} -> :ok
          {:ok, %{type: :symlink}} -> {:error, {:unsafe_blob_file_path, path, :symlink}}
          {:ok, %{type: type}} -> {:error, {:invalid_blob_file_path, path, type}}
          {:error, reason} -> {:error, reason}
        end
      end

      defp ensure_safe_segment_file_for_append(path) do
        case File.lstat(path) do
          {:ok, %{type: :regular}} -> :ok
          {:error, :enoent} -> :ok
          {:ok, %{type: :symlink}} -> {:error, {:unsafe_blob_segment_path, path, :symlink}}
          {:ok, %{type: type}} -> {:error, {:invalid_blob_segment_file, path, type}}
          {:error, reason} -> {:error, reason}
        end
      end

      defp safe_segment_lstat_size(path) do
        case File.lstat(path) do
          {:ok, %{type: :regular, size: size}} -> {:ok, size}
          {:ok, %{type: :symlink}} -> {:error, {:unsafe_blob_segment_path, path, :symlink}}
          {:ok, %{type: type}} -> {:error, {:invalid_blob_segment_file, path, type}}
          {:error, reason} -> {:error, reason}
        end
      end

      defp fsync_parent_after_mkdir(_dir, true), do: :ok

      defp fsync_parent_after_mkdir(dir, false) do
        # The first append segment in a shard must make the segments directory entry
        # durable before the segment file itself is fsynced.
        fsync_dir(Path.dirname(dir))
      end

      defp fsync_file(path) do
        case Process.get(:ferricstore_blob_store_fsync_file_hook) do
          fun when is_function(fun, 1) -> normalize_fsync(fun.(path))
          _ -> normalize_fsync(NIF.v2_fsync(path))
        end
      end

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
        case open_read_file(path) do
          {:ok, io} ->
            try do
              case hash_file(io, :crypto.hash_init(:sha256)) do
                {:ok, ^expected_checksum} -> :ok
                {:ok, _other_checksum} -> {:error, :checksum_mismatch}
                {:error, reason} -> {:error, reason}
              end
            after
              :file.close(io)
            end

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp read_segment_payload(path, offset, size, %BlobRef{} = ref) do
        case open_read_file(path) do
          {:ok, io} ->
            try do
              with :ok <- validate_open_segment_record(io, offset, size, ref),
                   {:ok, payload} <- pread_exact_open(io, offset, size),
                   :ok <- verify_checksum(ref, payload) do
                {:ok, payload}
              end
            after
              :file.close(io)
            end

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp read_blob_file_range(path, relative_offset, count) do
        case open_read_file(path) do
          {:ok, io} ->
            try do
              pread_exact_open(io, relative_offset, count)
            after
              :file.close(io)
            end

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp read_segment_payload_range(
             path,
             offset,
             size,
             %BlobRef{} = ref,
             relative_offset,
             count
           ) do
        case open_read_file(path) do
          {:ok, io} ->
            try do
              with :ok <- validate_open_segment_record(io, offset, size, ref),
                   :ok <-
                     maybe_validate_open_segment_payload_range(
                       io,
                       offset,
                       size,
                       ref,
                       relative_offset,
                       count
                     ) do
                pread_exact_open(io, offset + relative_offset, count)
              end
            after
              :file.close(io)
            end

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp verify_segment_record(path, offset, size, %BlobRef{} = ref) do
        case open_read_file(path) do
          {:ok, io} ->
            try do
              with :ok <- validate_open_segment_record(io, offset, size, ref),
                   :ok <- open_file_range_matches_ref?(io, offset, size, ref) do
                :ok
              end
            after
              :file.close(io)
            end

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp validate_segment_record_header(path, offset, size, %BlobRef{} = ref) do
        case open_read_file(path) do
          {:ok, io} ->
            try do
              validate_open_segment_record(io, offset, size, ref)
            after
              :file.close(io)
            end

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp validate_open_segment_record(
             io,
             offset,
             size,
             %BlobRef{version: 2, checksum: expected_checksum}
           ) do
        header_offset = offset - @segment_header_bytes

        with true <- header_offset >= 0,
             {:ok, header} when byte_size(header) == @segment_header_bytes <-
               :file.pread(io, header_offset, @segment_header_bytes),
             {:ok, ^size, ^expected_checksum} <- decode_segment_header(header) do
          :ok
        else
          _ -> {:error, :segment_header_mismatch}
        end
      end

      defp open_file_range_matches_ref?(io, offset, size, %BlobRef{checksum: expected_checksum}) do
        case hash_file_range(io, offset, size, :crypto.hash_init(:sha256)) do
          {:ok, ^expected_checksum} -> :ok
          {:ok, _other_checksum} -> {:error, :checksum_mismatch}
          {:error, reason} -> {:error, reason}
        end
      end

      defp maybe_validate_open_segment_payload_range(io, offset, size, ref, 0, count)
           when count == size,
           do: open_file_range_matches_ref?(io, offset, size, ref)

      defp maybe_validate_open_segment_payload_range(
             _io,
             _offset,
             _size,
             _ref,
             _relative_offset,
             _count
           ),
           do: :ok

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

      defp hash_file_range(_io, _offset, 0, hash_state), do: {:ok, :crypto.hash_final(hash_state)}

      defp hash_file_range(io, offset, remaining, hash_state) do
        read_size = min(@hash_chunk_bytes, remaining)

        case :file.pread(io, offset, read_size) do
          {:ok, chunk} when is_binary(chunk) and byte_size(chunk) == read_size ->
            hash_file_range(
              io,
              offset + read_size,
              remaining - read_size,
              :crypto.hash_update(hash_state, chunk)
            )

          {:ok, _short} ->
            {:error, :size_mismatch}

          :eof ->
            {:error, :size_mismatch}

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp recover_segment(path, can_truncate?) do
        with :ok <- ensure_safe_segment_file_for_recovery(path) do
          case File.open(path, [:read, :write, :raw, :binary]) do
            {:ok, io} ->
              try do
                with {:ok, size} <- file_size(io),
                     {:ok, valid_size} <- scan_segment(io, 0, size),
                     {:ok, truncated_bytes} <-
                       maybe_truncate_segment(io, path, size, valid_size, can_truncate?) do
                  {:ok, truncated_bytes}
                end
              after
                :file.close(io)
              end

            {:error, reason} ->
              {:error, reason}
          end
        end
      end

      defp ensure_safe_segment_file_for_recovery(path) do
        case File.lstat(path) do
          {:ok, %{type: :regular}} -> :ok
          {:ok, %{type: :symlink}} -> {:error, {:unsafe_blob_segment_path, path, :symlink}}
          {:ok, %{type: type}} -> {:error, {:invalid_blob_segment_file, path, type}}
          {:error, reason} -> {:error, reason}
        end
      end

      defp file_size(io) do
        with {:ok, current} <- :file.position(io, :cur),
             {:ok, size} <- :file.position(io, :eof),
             {:ok, _} <- :file.position(io, current) do
          {:ok, size}
        end
      end

      defp scan_segment(_io, offset, size) when offset == size, do: {:ok, offset}

      defp scan_segment(_io, offset, size) when size - offset < @segment_header_bytes,
        do: {:ok, offset}

      defp scan_segment(io, offset, size) do
        case :file.pread(io, offset, @segment_header_bytes) do
          {:ok, header} when byte_size(header) == @segment_header_bytes ->
            case decode_segment_header(header) do
              {:ok, payload_size, checksum} ->
                payload_offset = offset + @segment_header_bytes
                next_offset = payload_offset + payload_size

                cond do
                  next_offset > size ->
                    {:ok, offset}

                  segment_payload_matches?(io, payload_offset, payload_size, checksum) ->
                    scan_segment(io, next_offset, size)

                  true ->
                    {:ok, offset}
                end

              :error ->
                {:ok, offset}
            end

          {:ok, _short} ->
            {:ok, offset}

          :eof ->
            {:ok, offset}

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp decode_segment_header(
             <<@segment_header_magic::binary, size::unsigned-big-64, checksum::binary-size(32)>>
           ),
           do: {:ok, size, checksum}

      defp decode_segment_header(_header), do: :error

      defp segment_payload_matches?(io, payload_offset, payload_size, checksum) do
        case hash_file_range(io, payload_offset, payload_size, :crypto.hash_init(:sha256)) do
          {:ok, ^checksum} -> true
          _ -> false
        end
      end

      defp maybe_truncate_segment(_io, _path, size, size, _can_truncate?), do: {:ok, 0}

      defp maybe_truncate_segment(_io, path, _size, _valid_size, false) do
        {:error, {:corrupt_immutable_blob_segment, path}}
      end

      defp maybe_truncate_segment(io, path, size, valid_size, true) do
        with {:ok, _} <- :file.position(io, valid_size),
             :ok <- :file.truncate(io),
             :ok <- fsync_file(path) do
          {:ok, size - valid_size}
        end
      end

      defp validate_blob_range(size, relative_offset, count)
           when is_integer(size) and size >= 0 and is_integer(relative_offset) and
                  relative_offset >= 0 and is_integer(count) and count >= 0 and
                  relative_offset + count <= size,
           do: :ok

      defp validate_blob_range(_size, _relative_offset, _count), do: {:error, :invalid_blob_range}

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
    end
  end
end
