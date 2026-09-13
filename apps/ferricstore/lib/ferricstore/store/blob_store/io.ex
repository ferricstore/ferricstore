defmodule Ferricstore.Store.BlobStore.IO do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      alias Ferricstore.Bitcask.NIF
      alias Ferricstore.Store.BlobRef
      alias Ferricstore.Store.BlobStore.TableOwner

      # Torn-tail recovery scans in bounded windows. The overlap lets a magic
      # marker and its complete header straddle two windows without materializing
      # the whole suffix. Healthy segments continue to use the normal prefix scan.
      @torn_tail_probe_bytes 1_048_576
      @torn_tail_probe_overlap_bytes @segment_header_bytes - 1
      @torn_tail_probe_read_bytes @torn_tail_probe_bytes - @torn_tail_probe_overlap_bytes
      @torn_tail_probe_hash_bytes 32 * 1024 * 1024

      defp segment_path(data_dir, shard_index, segment_id) do
        Path.join([
          Ferricstore.DataDir.blob_shard_path(data_dir, shard_index),
          "segments",
          BlobRef.segment_filename(segment_id)
        ])
      end

      defp stat_regular_min_size(path, min_size) do
        case File.lstat(path) do
          {:ok, %{type: :regular, size: size}} when size >= min_size -> :ok
          {:ok, %{type: :regular}} -> {:error, :size_mismatch}
          {:ok, _other} -> {:error, :invalid_blob_file}
          {:error, reason} -> {:error, reason}
        end
      end

      defp pread_exact_open(_io, _offset, 0), do: {:ok, ""}

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

        with {:ok, expected_stat} <- safe_blob_file_stat(path),
             {:ok, io} <- open_blob_read_path(path, modes) do
          case verify_open_file_identity(io, path, expected_stat) do
            :ok ->
              {:ok, io}

            {:error, _reason} = error ->
              _ = :file.close(io)
              error
          end
        end
      end

      defp open_blob_read_path(path, modes) do
        case Process.get(:ferricstore_blob_store_open_read_hook) do
          fun when is_function(fun, 2) -> fun.(path, modes)
          _other -> File.open(path, modes)
        end
      end

      defp safe_blob_file_stat(path) do
        case File.lstat(path) do
          {:ok, %{type: :regular} = stat} -> {:ok, stat}
          {:ok, %{type: :symlink}} -> {:error, {:unsafe_blob_file_path, path, :symlink}}
          {:ok, %{type: type}} -> {:error, {:invalid_blob_file_path, path, type}}
          {:error, reason} -> {:error, reason}
        end
      end

      defp open_append_file(path) do
        case open_append_file(path, 1) do
          {:error, :enoent} -> {:error, :blob_segment_dir_missing}
          result -> result
        end
      end

      defp open_append_file(path, retry_count) do
        case File.lstat(path) do
          {:ok, %{type: :regular} = expected_stat} ->
            with {:ok, io} <- File.open(path, [:append, :raw, :binary]) do
              case verify_open_file_identity(io, path, expected_stat) do
                :ok ->
                  {:ok, io}

                {:error, _reason} = error ->
                  _ = :file.close(io)
                  error
              end
            end

          {:error, :enoent} ->
            case :file.open(
                   String.to_charlist(path),
                   [:append, :write, :raw, :binary, :exclusive]
                 ) do
              {:error, :eexist} when retry_count > 0 -> open_append_file(path, retry_count - 1)
              result -> result
            end

          {:ok, %{type: :symlink}} ->
            {:error, {:unsafe_blob_segment_path, path, :symlink}}

          {:ok, %{type: type}} ->
            {:error, {:invalid_blob_segment_file, path, type}}

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp verify_open_file_identity(io, path, %File.Stat{
             major_device: major_device,
             minor_device: minor_device,
             inode: inode
           }) do
        case :file.read_file_info(io) do
          {:ok, info}
          when elem(info, 2) == :regular and elem(info, 9) == major_device and
                 elem(info, 10) == minor_device and elem(info, 11) == inode ->
            :ok

          {:ok, _different_file} ->
            {:error, {:unsafe_blob_file_identity, path}}

          {:error, reason} ->
            {:error, reason}
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
             %BlobRef{checksum: expected_checksum}
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
        with {:ok, expected_stat} <- safe_segment_file_stat_for_recovery(path),
             {:ok, io} <- open_blob_recovery_path(path, [:read, :write, :raw, :binary]) do
          try do
            with :ok <- verify_open_file_identity(io, path, expected_stat),
                 {:ok, size} <- file_size(io) do
              case scan_segment(io, 0, size) do
                {:ok, valid_size} ->
                  maybe_recover_torn_segment(io, path, size, valid_size, can_truncate?)

                {:error, {:corrupt_blob_segment, offset}} ->
                  corrupt_segment_error(path, offset, can_truncate?)

                {:error, reason} ->
                  {:error, reason}
              end
            end
          after
            :file.close(io)
          end
        end
      end

      defp open_blob_recovery_path(path, modes) do
        case Process.get(:ferricstore_blob_store_open_recovery_hook) do
          fun when is_function(fun, 2) -> fun.(path, modes)
          _other -> File.open(path, modes)
        end
      end

      defp safe_segment_file_stat_for_recovery(path) do
        case File.lstat(path) do
          {:ok, %{type: :regular} = stat} -> {:ok, stat}
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

      defp maybe_recover_torn_segment(io, path, size, size, can_truncate?),
        do: maybe_truncate_segment(io, path, size, size, can_truncate?)

      defp maybe_recover_torn_segment(_io, path, _size, valid_size, false),
        do: corrupt_segment_error(path, valid_size, false)

      defp maybe_recover_torn_segment(io, path, size, valid_size, true) do
        case validate_torn_tail(io, size, valid_size) do
          :ok ->
            maybe_truncate_segment(io, path, size, valid_size, true)

          {:error, {:torn_tail_probe, reason}} ->
            corrupt_torn_tail_error(path, valid_size, reason)

          {:error, _reason} = error ->
            error
        end
      end

      # A complete header whose payload runs past EOF is normally a crash tail.
      # Before truncating it, stream the suffix for a complete magic/header/SHA
      # record that may have been appended after the torn body.
      defp validate_torn_tail(_io, size, valid_size)
           when size - valid_size < @segment_header_bytes,
           do: :ok

      defp validate_torn_tail(io, size, valid_size) do
        probe_torn_tail_stream(io, valid_size, size, <<>>, 0)
      end

      defp probe_torn_tail_stream(_io, offset, size, _overlap, _hash_bytes)
           when offset >= size,
           do: :ok

      defp probe_torn_tail_stream(io, offset, size, overlap, hash_bytes) do
        read_size = min(@torn_tail_probe_read_bytes, size - offset)

        case :file.pread(io, offset, read_size) do
          {:ok, chunk} when is_binary(chunk) and byte_size(chunk) == read_size ->
            window = overlap <> chunk
            window_start = offset - byte_size(overlap)

            case probe_torn_tail_window(
                   io,
                   window,
                   window_start,
                   size,
                   hash_bytes,
                   0
                 ) do
              {:ok, next_hash_bytes} ->
                next_offset = offset + read_size

                if next_offset >= size do
                  :ok
                else
                  next_overlap =
                    binary_part(
                      window,
                      byte_size(window) - @torn_tail_probe_overlap_bytes,
                      @torn_tail_probe_overlap_bytes
                    )

                  probe_torn_tail_stream(io, next_offset, size, next_overlap, next_hash_bytes)
                end

              {:error, _reason} = error ->
                error
            end

          {:ok, _short} ->
            {:error, :size_mismatch}

          :eof ->
            {:error, :size_mismatch}

          {:error, reason} ->
            {:error, reason}
        end
      end

      defp probe_torn_tail_window(
             io,
             window,
             window_start,
             size,
             hash_bytes,
             search_offset
           ) do
        window_size = byte_size(window)

        if search_offset + @segment_header_bytes > window_size do
          {:ok, hash_bytes}
        else
          case :binary.match(
                 window,
                 @segment_header_magic,
                 scope: {search_offset, window_size - search_offset}
               ) do
            :nomatch ->
              {:ok, hash_bytes}

            {candidate_offset, _magic_size} ->
              candidate_end = candidate_offset + @segment_header_bytes
              candidate_absolute_offset = window_start + candidate_offset

              cond do
                candidate_end > window_size ->
                  {:ok, hash_bytes}

                true ->
                  header = binary_part(window, candidate_offset, @segment_header_bytes)

                  case decode_segment_header(header) do
                    {:ok, payload_size, checksum} ->
                      payload_offset = candidate_absolute_offset + @segment_header_bytes

                      cond do
                        payload_offset > size or payload_size > size - payload_offset ->
                          probe_torn_tail_window(
                            io,
                            window,
                            window_start,
                            size,
                            hash_bytes,
                            candidate_offset + 1
                          )

                        hash_bytes + payload_size > @torn_tail_probe_hash_bytes ->
                          {:error, {:torn_tail_probe, :hash_budget_exceeded}}

                        true ->
                          case hash_torn_tail_candidate(
                                 io,
                                 window,
                                 candidate_offset,
                                 payload_offset,
                                 payload_size
                               ) do
                            {:ok, ^checksum} ->
                              {:error, {:torn_tail_probe, :complete_record_found}}

                            {:ok, _other_checksum} ->
                              probe_torn_tail_window(
                                io,
                                window,
                                window_start,
                                size,
                                hash_bytes + payload_size,
                                candidate_offset + 1
                              )

                            {:error, reason} ->
                              {:error, reason}
                          end
                      end

                    :error ->
                      probe_torn_tail_window(
                        io,
                        window,
                        window_start,
                        size,
                        hash_bytes,
                        candidate_offset + 1
                      )
                  end
              end
          end
        end
      end

      defp hash_torn_tail_candidate(io, window, candidate_offset, payload_offset, payload_size) do
        payload_end = candidate_offset + @segment_header_bytes + payload_size

        if payload_end <= byte_size(window) do
          payload = binary_part(window, candidate_offset + @segment_header_bytes, payload_size)
          {:ok, :crypto.hash(:sha256, payload)}
        else
          hash_file_range(io, payload_offset, payload_size, :crypto.hash_init(:sha256))
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

                  true ->
                    case segment_payload_matches?(io, payload_offset, payload_size, checksum) do
                      :ok -> scan_segment(io, next_offset, size)
                      {:error, :checksum_mismatch} -> {:error, {:corrupt_blob_segment, offset}}
                      {:error, reason} -> {:error, reason}
                    end
                end

              :error ->
                {:error, {:corrupt_blob_segment, offset}}
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
          {:ok, ^checksum} -> :ok
          {:ok, _other_checksum} -> {:error, :checksum_mismatch}
          {:error, reason} -> {:error, reason}
        end
      end

      defp corrupt_segment_error(path, _offset, false),
        do: {:error, {:corrupt_immutable_blob_segment, path}}

      defp corrupt_segment_error(path, offset, true),
        do: {:error, {:corrupt_blob_segment, path, offset}}

      defp corrupt_torn_tail_error(path, offset, reason),
        do: {:error, {:corrupt_blob_segment, path, offset, reason}}

      defp maybe_truncate_segment(_io, _path, size, size, _can_truncate?), do: {:ok, 0}

      defp maybe_truncate_segment(_io, path, _size, _valid_size, false) do
        {:error, {:corrupt_immutable_blob_segment, path}}
      end

      defp maybe_truncate_segment(io, path, size, valid_size, true) do
        with {:ok, current_size} <- file_size(io),
             true <- current_size == size,
             {:ok, _} <- :file.position(io, valid_size),
             :ok <- :file.truncate(io),
             :ok <- :file.sync(io) do
          {:ok, size - valid_size}
        else
          false -> {:error, {:blob_segment_changed, path}}
          {:error, reason} -> {:error, reason}
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
