defmodule FerricstoreServer.Connection.Sendfile.IO do
  @moduledoc false

  @file_stream_chunk_bytes Application.compile_env(
                              :ferricstore_server,
                              :file_stream_chunk_bytes,
                              65_536
                            )

  @bitcask_header_size 26
  @bitcask_tombstone_value_size 4_294_967_295
  @blob_segment_header_magic <<0, ?F, ?S, ?B, ?L, ?O, ?G, 1>>
  @blob_segment_header_bytes 48

  def pread_file_range(path, offset, size) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case file_pread(fd, offset, size) do
            {:ok, value} when byte_size(value) == size -> {:ok, value}
            _ -> :fallback
          end
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :fallback
    end
  end

  def pread_file_ref_range(path, value_offset, value_size, relative_offset, count) do
    if blob_file_ref_path?(path) do
      read_offset = value_offset + relative_offset

      case :file.open(path, [:read, :raw, :binary]) do
        {:ok, fd} ->
          try do
            with :ok <- validate_open_blob_file_ref(path, fd, value_offset, value_size, :sendfile),
                 {:ok, value} when byte_size(value) == count <-
                   file_pread(fd, read_offset, count) do
              {:ok, value}
            else
              _other -> :fallback
            end
          after
            :file.close(fd)
          end

        {:error, _reason} ->
          :fallback
      end
    else
      pread_file_range(path, value_offset + relative_offset, count)
    end
  end

  def pread_file_ref_value(validate_key, path, offset, size) do
    if blob_file_ref_path?(path) do
      pread_blob_file_ref_value(path, offset, size)
    else
      pread_bitcask_file_ref_value(validate_key, path, offset, size)
    end
  end

  def pread_blob_file_ref_value(path, value_offset, size) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case Path.extname(path) do
            ".blob" -> pread_legacy_blob_file_ref_value(path, fd, value_offset, size)
            ".bloblog" -> pread_segment_blob_file_ref_value(fd, value_offset, size)
            _other -> :fallback
          end
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :fallback
    end
  end

  def pread_legacy_blob_file_ref_value(path, fd, value_offset, size) do
    with true <- value_offset == 0,
         {:ok, ^size} <- :file.position(fd, :eof),
         {:ok, checksum} <- blob_checksum_from_path(path),
         {:ok, payload} when is_binary(payload) and byte_size(payload) == size <-
           file_pread(fd, 0, size),
         ^checksum <- :crypto.hash(:sha256, payload) do
      {:ok, payload}
    else
      _other -> :fallback
    end
  end

  def pread_segment_blob_file_ref_value(fd, value_offset, size) do
    header_offset = value_offset - @blob_segment_header_bytes

    with true <- header_offset >= 0,
         {:ok, header} when byte_size(header) == @blob_segment_header_bytes <-
           file_pread(fd, header_offset, @blob_segment_header_bytes),
         {:ok, ^size, expected_checksum} <- decode_blob_segment_header(header),
         {:ok, payload} when is_binary(payload) and byte_size(payload) == size <-
           file_pread(fd, value_offset, size),
         ^expected_checksum <- :crypto.hash(:sha256, payload) do
      {:ok, payload}
    else
      _other -> :fallback
    end
  end

  def blob_checksum_from_path(path) do
    hex = Path.basename(path, ".blob")

    with 64 <- byte_size(hex),
         {:ok, checksum} <- Base.decode16(hex, case: :lower) do
      {:ok, checksum}
    else
      _ -> :error
    end
  end

  def pread_bitcask_file_ref_value(validate_key, path, offset, size) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case validate_open_file_ref(path, fd, validate_key, offset, size) do
            :ok ->
              case file_pread(fd, offset, size) do
                {:ok, value} when is_binary(value) and byte_size(value) == size -> {:ok, value}
                _ -> :fallback
              end

            :mismatch ->
              :fallback
          end
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :fallback
    end
  end

  def normalize_byte_range(0, _start_idx, _end_idx), do: :empty

  def normalize_byte_range(size, start_idx, end_idx) when size > 0 do
    start_norm = if start_idx < 0, do: max(size + start_idx, 0), else: start_idx
    end_norm = if end_idx < 0, do: size + end_idx, else: end_idx

    start_clamped = min(start_norm, size)
    end_clamped = min(end_norm, size - 1)

    if start_clamped > end_clamped do
      :empty
    else
      count = end_clamped - start_clamped + 1
      {start_clamped, count}
    end
  end

  def slice_value(value, start_idx, end_idx) do
    case normalize_byte_range(byte_size(value), start_idx, end_idx) do
      :empty -> ""
      {offset, count} -> binary_part(value, offset, count)
    end
  end
  def validate_open_file_ref(path, fd, key, value_offset, value_size),
    do: validate_open_file_ref(path, fd, key, value_offset, value_size, :verify_payload)

  def validate_open_file_ref(path, fd, key, value_offset, value_size, mode) do
    if blob_file_ref_path?(path) do
      validate_open_blob_file_ref(path, fd, value_offset, value_size, mode)
    else
      validate_open_value_ref(fd, key, value_offset, value_size)
    end
  end

  def blob_file_ref_path?(path) when is_binary(path),
    do: Path.extname(path) in [".blob", ".bloblog"]

  def blob_file_ref_path?(_path), do: false

  def validate_open_file_range(_path, _fd, _offset, _size, :none, _mode), do: :ok

  def validate_open_file_range(
         path,
         fd,
         _offset,
         _size,
         {:blob, value_offset, value_size},
         mode
       ),
       do: validate_open_blob_file_ref(path, fd, value_offset, value_size, mode)

  def validate_open_blob_file_ref(path, fd, value_offset, value_size, mode)
       when is_integer(value_offset) and value_offset >= 0 and is_integer(value_size) and
              value_size >= 0 do
    case Path.extname(path) do
      ".blob" -> validate_open_legacy_blob_file_ref(path, fd, value_offset, value_size)
      ".bloblog" -> validate_open_segment_blob_file_ref(fd, value_offset, value_size, mode)
      _other -> :mismatch
    end
  end

  def validate_open_blob_file_ref(_path, _fd, _value_offset, _value_size, _mode),
    do: :mismatch

  def validate_open_legacy_blob_file_ref(path, fd, value_offset, value_size) do
    with true <- value_offset == 0,
         {:ok, ^value_size} <- :file.position(fd, :eof),
         {:ok, expected_checksum} <- blob_checksum_from_path(path),
         {:ok, ^expected_checksum} <- hash_open_file(fd) do
      :ok
    else
      _ -> :mismatch
    end
  end

  def validate_open_segment_blob_file_ref(fd, value_offset, value_size, mode) do
    header_offset = value_offset - @blob_segment_header_bytes

    with true <- header_offset >= 0,
         {:ok, header} when byte_size(header) == @blob_segment_header_bytes <-
           file_pread(fd, header_offset, @blob_segment_header_bytes),
         {:ok, ^value_size, expected_checksum} <- decode_blob_segment_header(header),
         :ok <-
           maybe_verify_segment_blob_payload(
             fd,
             value_offset,
             value_size,
             expected_checksum,
             mode
           ) do
      :ok
    else
      _ -> :mismatch
    end
  end

  def maybe_verify_segment_blob_payload(
         fd,
         value_offset,
         value_size,
         expected_checksum,
         mode
       ) do
    # Blob side-channel files are addressed by checksum. Validate the payload
    # before sending any RESP header so TCP sendfile cannot expose corrupt bytes.
    started = System.monotonic_time()

    result =
      case hash_open_file_range(fd, value_offset, value_size) do
        {:ok, ^expected_checksum} -> :ok
        _other -> :mismatch
      end

    emit_blob_checksum_validation(value_size, started, mode, result)
    result
  end

  def emit_blob_checksum_validation(bytes, started, mode, result) do
    duration =
      System.monotonic_time()
      |> Kernel.-(started)
      |> System.convert_time_unit(:native, :microsecond)

    :telemetry.execute(
      [:ferricstore, :server, :sendfile, :blob_checksum],
      %{bytes: bytes, duration_us: duration},
      %{mode: mode, result: result}
    )
  end

  def decode_blob_segment_header(
         <<@blob_segment_header_magic::binary, size::unsigned-big-64, checksum::binary-size(32)>>
       ),
       do: {:ok, size, checksum}

  def decode_blob_segment_header(_header), do: :error

  def hash_open_file(fd) do
    with {:ok, 0} <- :file.position(fd, 0) do
      hash_open_file(fd, :crypto.hash_init(:sha256))
    end
  end

  def hash_open_file_range(_fd, _offset, 0),
    do: {:ok, :crypto.hash(:sha256, <<>>)}

  def hash_open_file_range(fd, offset, size) do
    hash_open_file_range(fd, offset, size, :crypto.hash_init(:sha256))
  end

  def hash_open_file_range(_fd, _offset, 0, hash_state),
    do: {:ok, :crypto.hash_final(hash_state)}

  def hash_open_file_range(fd, offset, remaining, hash_state) do
    read_size = min(remaining, @file_stream_chunk_bytes)

    case file_pread(fd, offset, read_size) do
      {:ok, chunk} when is_binary(chunk) and byte_size(chunk) == read_size ->
        hash_open_file_range(
          fd,
          offset + read_size,
          remaining - read_size,
          :crypto.hash_update(hash_state, chunk)
        )

      _other ->
        :mismatch
    end
  end

  def hash_open_file(fd, hash_state) do
    case :file.read(fd, @file_stream_chunk_bytes) do
      {:ok, chunk} when is_binary(chunk) and byte_size(chunk) > 0 ->
        hash_open_file(fd, :crypto.hash_update(hash_state, chunk))

      :eof ->
        {:ok, :crypto.hash_final(hash_state)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def validate_open_value_ref(fd, key, value_offset, value_size)
       when is_binary(key) and is_integer(value_offset) and is_integer(value_size) do
    key_size = byte_size(key)

    with true <- value_size <= @bitcask_tombstone_value_size,
         record_offset when record_offset >= 0 <-
           value_offset - @bitcask_header_size - key_size,
         {:ok, header} when byte_size(header) == @bitcask_header_size <-
           file_pread(fd, record_offset, @bitcask_header_size),
         ^key_size <- decode_le_unsigned(header, 20, 2),
         ^value_size <- decode_le_unsigned(header, 22, 4),
         {:ok, ^key} <- file_pread(fd, record_offset + @bitcask_header_size, key_size) do
      :ok
    else
      _ -> :mismatch
    end
  end

  def validate_open_value_ref(_fd, _key, _value_offset, _value_size), do: :mismatch

  def cached_file_open(path, %{files: files} = file_cache) do
    case Map.fetch(files, path) do
      {:ok, fd} ->
        {:ok, fd, file_cache}

      :error ->
        case file_open(path) do
          {:ok, fd} -> {:ok, fd, %{file_cache | files: Map.put(files, path, fd)}}
          {:error, _reason} = error -> error
        end
    end
  end

  @doc false
  def new_file_cache, do: %{files: %{}, validations: %{}}

  @doc false
  def close_file_cache(%{files: files}) do
    Enum.each(files, fn {_path, fd} -> :file.close(fd) end)
  end

  def cached_validate_file_ref(path, fd, key, offset, size, mode, file_cache) do
    cache_key = validation_cache_key(path, key, offset, size, mode)

    cached_validate(cache_key, file_cache, fn ->
      validate_open_file_ref(path, fd, key, offset, size, mode)
    end)
  end

  def cached_validate_file_range(_path, _fd, _offset, _size, :none, _mode, file_cache),
    do: {:ok, file_cache}

  def cached_validate_file_range(
         path,
         fd,
         _offset,
         _size,
         {:blob, value_offset, value_size},
         mode,
         file_cache
       ) do
    cache_key = validation_cache_key(path, nil, value_offset, value_size, mode)

    cached_validate(cache_key, file_cache, fn ->
      validate_open_blob_file_ref(path, fd, value_offset, value_size, mode)
    end)
  end

  def cached_validate(cache_key, %{validations: validations} = file_cache, validate_fun) do
    case Map.fetch(validations, cache_key) do
      {:ok, result} ->
        {result, file_cache}

      :error ->
        result = validate_fun.()
        {result, %{file_cache | validations: Map.put(validations, cache_key, result)}}
    end
  end

  def validation_cache_key(path, key, offset, size, mode) do
    if blob_file_ref_path?(path) do
      {:blob_ref, path, offset, size, mode}
    else
      {:value_ref, path, key, offset, size, mode}
    end
  end

  def file_open(path) do
    modes = [:read, :raw, :binary]

    case Process.get(:ferricstore_sendfile_open_hook) do
      fun when is_function(fun, 2) -> fun.(path, modes)
      _other -> :file.open(path, modes)
    end
  end

  def file_pread(fd, offset, size) do
    case Process.get(:ferricstore_sendfile_pread_hook) do
      fun when is_function(fun, 3) -> fun.(fd, offset, size)
      _other -> :file.pread(fd, offset, size)
    end
  end

  def decode_le_unsigned(binary, offset, size) do
    binary
    |> binary_part(offset, size)
    |> :binary.decode_unsigned(:little)
  end

  def emit_sendfile_result({:sent, _state}, size, state) do
    emit_sendfile(:ok, size, state, %{})
  end

  def emit_sendfile_result(:fallback, size, state) do
    emit_sendfile(:fallback, size, state, %{})
  end

  def emit_sendfile_result({:error_after_header, reason}, size, state) do
    emit_sendfile(:error_after_header, size, state, %{reason: reason})
  end

  def emit_sendfile(result, size, state, metadata) do
    :telemetry.execute(
      [:ferricstore, :server, :sendfile],
      %{bytes: size},
      Map.merge(%{result: result, client_id: state.client_id}, metadata)
    )
  end

  def emit_file_stream_result({:sent, _state, chunks}, size, state) do
    emit_file_stream(:ok, size, chunks, state, %{})
  end

  def emit_file_stream_result(:fallback, size, state) do
    emit_file_stream(:fallback, size, 0, state, %{})
  end

  def emit_file_stream_result({:error_after_header, reason, chunks}, size, state) do
    emit_file_stream(:error_after_header, size, chunks, state, %{reason: reason})
  end

  def emit_file_stream(result, size, chunks, state, metadata) do
    :telemetry.execute(
      [:ferricstore, :server, :file_stream],
      %{bytes: size, chunks: chunks},
      Map.merge(
        %{result: result, client_id: state.client_id, transport: state.transport},
        metadata
      )
    )
  end
end
