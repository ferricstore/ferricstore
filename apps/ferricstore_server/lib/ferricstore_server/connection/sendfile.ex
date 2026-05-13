defmodule FerricstoreServer.Connection.Sendfile do
  @moduledoc "Large cold-value response streaming for GET/MGET/GETRANGE."

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Send, as: ConnSend
  alias FerricstoreServer.Connection.TcpOpts
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking

  @sendfile_threshold_bytes Application.compile_env(
                              :ferricstore_server,
                              :sendfile_threshold,
                              65_536
                            )

  @file_stream_chunk_bytes Application.compile_env(
                             :ferricstore_server,
                             :file_stream_chunk_bytes,
                             65_536
                           )

  @bitcask_header_size 26
  @bitcask_tombstone_value_size 4_294_967_295
  @blob_segment_header_magic <<0, ?F, ?S, ?B, ?L, ?O, ?G, 1>>
  @blob_segment_header_bytes 48

  @spec threshold_bytes() :: pos_integer()
  def threshold_bytes, do: @sendfile_threshold_bytes

  @spec file_stream_chunk_bytes() :: pos_integer()
  def file_stream_chunk_bytes, do: @file_stream_chunk_bytes

  @doc """
  Handles the GET command with sendfile optimization for `:ranch_tcp` transport
  and bounded chunk streaming for encrypted transports.
  Falls back to normal dispatch for non-sendfile cases.

  The `dispatch_normal_fn` parameter is a function `(cmd, args, state) -> result`
  used as fallback when sendfile is not applicable.
  """
  def dispatch_get([key], state, dispatch_normal_fn) do
    if in_pubsub_mode?(state) do
      {:continue,
       Encoder.encode(
         {:error,
          "ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"}
       ), state}
    else
      fast_get(key, state, dispatch_normal_fn)
    end
  end

  @doc """
  Handles MGET without materializing large cold bulk elements.

  RESP arrays still need an array header, so this streams the header first,
  then sends normal encoded elements for nil/hot/small values and file-backed
  bulk elements for large cold refs. Plain TCP uses sendfile for those file
  refs; encrypted transports stream bounded chunks.
  """
  def dispatch_mget(keys, state, dispatch_normal_fn) when is_list(keys) and keys != [] do
    if in_pubsub_mode?(state) do
      dispatch_normal_fn.("MGET", keys, state)
    else
      fast_mget(keys, state, dispatch_normal_fn)
    end
  end

  def dispatch_mget(keys, state, dispatch_normal_fn), do: dispatch_normal_fn.("MGET", keys, state)

  @doc """
  Handles GETRANGE without materializing the full cold value.

  Large cold slices are streamed from file refs. Smaller cold slices are read
  with a bounded pread of only the requested bytes.
  """
  def dispatch_getrange(args, key, start_idx, end_idx, state, dispatch_normal_fn)
      when is_list(args) and is_binary(key) and is_integer(start_idx) and is_integer(end_idx) do
    if in_pubsub_mode?(state) do
      dispatch_normal_fn.(state)
    else
      fast_getrange(args, key, start_idx, end_idx, state, dispatch_normal_fn)
    end
  end

  # ---------------------------------------------------------------------------
  # Internal
  # ---------------------------------------------------------------------------

  defp fast_get(key, state, dispatch_normal_fn) do
    ns = state.sandbox_namespace
    lookup_key = if ns, do: ns <> key, else: key

    case Router.get_with_file_ref(state.instance_ctx, lookup_key) do
      {:hot, value} ->
        encode_get_result(value, key, state)

      {:cold_ref, path, offset, size} when size >= @sendfile_threshold_bytes ->
        stream_large_get_ref(key, lookup_key, path, offset, size, state, dispatch_normal_fn)

      {:cold_ref, path, offset, size} ->
        case pread_file_ref_value(lookup_key, path, offset, size) do
          {:ok, value} -> encode_get_result(value, key, state)
          :fallback -> dispatch_normal_fn.("GET", [key], state)
        end

      {:cold_value, value} ->
        encode_get_result(value, key, state)

      :miss ->
        encode_get_result(nil, key, state)
    end
  end

  defp stream_large_get_ref(
         client_key,
         lookup_key,
         path,
         offset,
         size,
         %{transport: :ranch_tcp} = state,
         dispatch_normal_fn
       ) do
    handle_sendfile_result(
      send_file_ref(client_key, lookup_key, path, offset, size, state),
      client_key,
      state,
      dispatch_normal_fn
    )
  end

  defp stream_large_get_ref(client_key, lookup_key, path, offset, size, state, dispatch_normal_fn) do
    handle_file_stream_result(
      stream_file_ref(client_key, lookup_key, path, offset, size, state),
      client_key,
      state,
      dispatch_normal_fn
    )
  end

  defp encode_get_result(value, key, state) do
    new_state = ConnTracking.maybe_track_read("GET", [key], value, state)
    ConnTracking.maybe_notify_keyspace("GET", [key], value)
    ConnTracking.maybe_notify_tracking("GET", [key], value, state)
    {:continue, Encoder.encode(value), new_state}
  end

  defp fast_mget(keys, state, dispatch_normal_fn) do
    lookup_keys = namespace_keys(state.sandbox_namespace, keys)

    case fetch_mget_with_file_refs(state.instance_ctx, lookup_keys) do
      {:ok, results} ->
        if Enum.any?(results, &match?({:file_ref, _, _, _}, &1)) do
          handle_mget_sendfile_result(
            send_mget_array(keys, lookup_keys, results, state),
            keys,
            state,
            dispatch_normal_fn
          )
        else
          new_state = ConnTracking.maybe_track_read("MGET", keys, results, state)
          {:continue, Encoder.encode(results), new_state}
        end

      :error ->
        dispatch_normal_fn.("MGET", keys, state)
    end
  end

  defp fetch_mget_with_file_refs(instance_ctx, lookup_keys) do
    {:ok, Router.batch_get_with_file_refs(instance_ctx, lookup_keys, threshold_bytes())}
  catch
    _, _ -> :error
  end

  defp fast_getrange(args, key, start_idx, end_idx, state, dispatch_normal_fn) do
    case getrange_cold_response(args, key, start_idx, end_idx, state) do
      {:value, value} ->
        new_state = ConnTracking.maybe_track_read("GETRANGE", args, value, state)
        {:continue, Encoder.encode(value), new_state}

      {:file_range, ^args, ^key, ^start_idx, ^end_idx, path, offset, count, validator} ->
        handle_getrange_send_result(
          send_file_range_response(args, path, offset, count, validator, state),
          state,
          dispatch_normal_fn
        )

      :fallback ->
        dispatch_normal_fn.(state)
    end
  end

  @doc false
  def getrange_cold_response(args, key, start_idx, end_idx, state) do
    lookup_key = namespace_key(state.sandbox_namespace, key)

    case fetch_get_with_file_ref(state.instance_ctx, lookup_key) do
      {:ok, {:cold_ref, path, value_offset, value_size}} ->
        case normalize_byte_range(value_size, start_idx, end_idx) do
          :empty ->
            {:value, ""}

          {relative_offset, count} when count >= @sendfile_threshold_bytes ->
            validator = file_range_validator(path, value_offset, value_size)

            {:file_range, args, key, start_idx, end_idx, path, value_offset + relative_offset,
             count, validator}

          {relative_offset, count} ->
            case pread_file_ref_range(path, value_offset, value_size, relative_offset, count) do
              {:ok, value} -> {:value, value}
              :fallback -> :fallback
            end
        end

      _other ->
        :fallback
    end
  end

  defp fetch_get_with_file_ref(instance_ctx, lookup_key) do
    {:ok, Router.get_with_file_ref(instance_ctx, lookup_key)}
  catch
    _, _ -> :error
  end

  defp file_range_validator(path, value_offset, value_size) do
    if blob_file_ref_path?(path), do: {:blob, value_offset, value_size}, else: :none
  end

  defp default_file_range_validator(path, offset, size) do
    if blob_file_ref_path?(path), do: {:blob, offset, size}, else: :none
  end

  @doc false
  def materialize_getrange(key, start_idx, end_idx, state) do
    case Router.get(state.instance_ctx, namespace_key(state.sandbox_namespace, key)) do
      nil ->
        ""

      value when is_binary(value) ->
        slice_value(value, start_idx, end_idx)

      value when is_integer(value) ->
        value |> Integer.to_string() |> slice_value(start_idx, end_idx)

      value when is_float(value) ->
        value |> Float.to_string() |> slice_value(start_idx, end_idx)
    end
  end

  defp handle_getrange_send_result({:sent, new_state}, _state, _fn),
    do: {:continue, "", new_state}

  defp handle_getrange_send_result({:error_after_header, _reason}, state, _fn),
    do: {:quit, "", state}

  defp handle_getrange_send_result(:fallback, state, dispatch_normal_fn),
    do: dispatch_normal_fn.(state)

  @doc false
  def send_file_range(args, path, offset, size, state),
    do:
      send_file_range(
        args,
        path,
        offset,
        size,
        default_file_range_validator(path, offset, size),
        state
      )

  def send_file_range(args, path, offset, size, validator, state) do
    result = do_sendfile_range(path, offset, size, validator, state)
    emit_sendfile_result(result, size, state)

    case result do
      {:sent, new_state} ->
        {:sent, ConnTracking.maybe_track_read("GETRANGE", args, :sendfile_ok, new_state)}

      other ->
        other
    end
  end

  @doc false
  def send_file_range_response(args, path, offset, size, %{transport: :ranch_tcp} = state) do
    validator = default_file_range_validator(path, offset, size)
    send_file_range(args, path, offset, size, validator, state)
  end

  def send_file_range_response(args, path, offset, size, state) do
    validator = default_file_range_validator(path, offset, size)
    send_file_range_response(args, path, offset, size, validator, state)
  end

  def send_file_range_response(
        args,
        path,
        offset,
        size,
        validator,
        %{transport: :ranch_tcp} = state
      ),
      do: send_file_range(args, path, offset, size, validator, state)

  def send_file_range_response(args, path, offset, size, validator, state) do
    result = do_stream_file_get(path, offset, size, validator, state)
    emit_file_stream_result(result, size, state)

    case result do
      {:sent, new_state, _chunks} ->
        {:sent, ConnTracking.maybe_track_read("GETRANGE", args, :file_stream_ok, new_state)}

      :fallback ->
        :fallback

      {:error_after_header, reason, _chunks} ->
        {:error_after_header, reason}
    end
  end

  defp do_sendfile_range(path, offset, size, validator, state) do
    socket = state.socket

    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case validate_open_file_range(path, fd, offset, size, validator) do
            :ok -> send_file_range_with_cork(socket, fd, offset, size, state)
            :mismatch -> :fallback
          end
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :fallback
    end
  end

  defp send_file_range_with_cork(socket, fd, offset, size, state) do
    header = [?$, Integer.to_string(size), "\r\n"]
    TcpOpts.set_cork(socket, true)

    case :gen_tcp.send(socket, header) do
      :ok ->
        result = send_file_range_and_trailer(socket, fd, offset, size, state)
        TcpOpts.set_cork(socket, false)
        result

      {:error, _reason} ->
        TcpOpts.set_cork(socket, false)
        :fallback
    end
  end

  defp send_file_range_and_trailer(socket, fd, offset, size, state) do
    case :file.sendfile(fd, socket, offset, size, []) do
      {:ok, ^size} ->
        case :gen_tcp.send(socket, "\r\n") do
          :ok -> {:sent, state}
          {:error, reason} -> {:error_after_header, reason}
        end

      {:ok, sent} when sent < size ->
        {:error_after_header, :partial_send}

      {:error, reason} ->
        {:error_after_header, reason}
    end
  end

  defp pread_file_range(path, offset, size) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case :file.pread(fd, offset, size) do
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

  defp pread_file_ref_range(path, value_offset, value_size, relative_offset, count) do
    if blob_file_ref_path?(path) do
      with {:ok, payload} <- pread_blob_file_ref_value(path, value_size) do
        {:ok, binary_part(payload, relative_offset, count)}
      end
    else
      pread_file_range(path, value_offset + relative_offset, count)
    end
  end

  defp pread_file_ref_value(validate_key, path, 0, size) do
    if blob_file_ref_path?(path) do
      pread_blob_file_ref_value(path, size)
    else
      pread_bitcask_file_ref_value(validate_key, path, 0, size)
    end
  end

  defp pread_file_ref_value(validate_key, path, offset, size) do
    if blob_file_ref_path?(path) do
      :fallback
    else
      pread_bitcask_file_ref_value(validate_key, path, offset, size)
    end
  end

  defp pread_blob_file_ref_value(path, size) do
    with {:ok, checksum} <- blob_checksum_from_path(path),
         {:ok, payload} when is_binary(payload) and byte_size(payload) == size <- File.read(path),
         ^checksum <- :crypto.hash(:sha256, payload) do
      {:ok, payload}
    else
      _ -> :fallback
    end
  end

  defp blob_checksum_from_path(path) do
    hex = Path.basename(path, ".blob")

    with 64 <- byte_size(hex),
         {:ok, checksum} <- Base.decode16(hex, case: :lower) do
      {:ok, checksum}
    else
      _ -> :error
    end
  end

  defp pread_bitcask_file_ref_value(validate_key, path, offset, size) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case validate_open_file_ref(path, fd, validate_key, offset, size) do
            :ok ->
              case :file.pread(fd, offset, size) do
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

  defp normalize_byte_range(0, _start_idx, _end_idx), do: :empty

  defp normalize_byte_range(size, start_idx, end_idx) when size > 0 do
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

  defp slice_value(value, start_idx, end_idx) do
    case normalize_byte_range(byte_size(value), start_idx, end_idx) do
      :empty -> ""
      {offset, count} -> binary_part(value, offset, count)
    end
  end

  defp handle_mget_sendfile_result({:sent, new_state}, _keys, _state, _fn),
    do: {:continue, "", new_state}

  defp handle_mget_sendfile_result({:error_after_header, _reason}, _keys, state, _fn),
    do: {:quit, "", state}

  defp handle_mget_sendfile_result(:fallback, keys, state, dispatch_normal_fn),
    do: dispatch_normal_fn.("MGET", keys, state)

  defp send_mget_array(keys, lookup_keys, results, %{transport: :ranch_tcp} = state) do
    socket = state.socket
    TcpOpts.set_cork(socket, true)

    try do
      case :gen_tcp.send(socket, ["*", Integer.to_string(length(keys)), "\r\n"]) do
        :ok ->
          stream_mget_elements(keys, lookup_keys, results, state)

        {:error, _reason} ->
          :fallback
      end
    after
      TcpOpts.set_cork(socket, false)
    end
  end

  defp send_mget_array(keys, lookup_keys, results, state) do
    case ConnSend.send(
           state.socket,
           state.transport,
           ["*", Integer.to_string(length(keys)), "\r\n"],
           :mget_stream_header,
           %{client_id: state.client_id}
         ) do
      :ok ->
        stream_mget_elements(keys, lookup_keys, results, state)

      {:error, _reason} ->
        :fallback
    end
  end

  defp stream_mget_elements(keys, lookup_keys, results, state) do
    keys
    |> Enum.zip(lookup_keys)
    |> Enum.zip(results)
    |> Enum.reduce_while({:sent, state}, fn
      {{key, lookup_key}, {:file_ref, path, offset, size}}, {:sent, acc_state} ->
        case send_file_ref_element_response(key, lookup_key, path, offset, size, acc_state) do
          {:sent, new_state} ->
            {:cont, {:sent, new_state}}

          :fallback ->
            case send_stream_response(
                   acc_state,
                   Encoder.encode(Router.get(acc_state.instance_ctx, lookup_key)),
                   :mget_stream_fallback
                 ) do
              :ok -> {:cont, {:sent, acc_state}}
              {:error, reason} -> {:halt, {:error_after_header, reason}}
            end

          {:error_after_header, _reason} = error ->
            {:halt, error}
        end

      {{_key, _lookup_key}, value}, {:sent, acc_state} ->
        case send_stream_response(acc_state, Encoder.encode(value), :mget_stream_element) do
          :ok -> {:cont, {:sent, acc_state}}
          {:error, reason} -> {:halt, {:error_after_header, reason}}
        end
    end)
    |> case do
      {:sent, new_state} ->
        {:sent, ConnTracking.maybe_track_read("MGET", keys, :sendfile_ok, new_state)}

      other ->
        other
    end
  end

  defp send_stream_response(%{transport: :ranch_tcp, socket: socket}, iodata, _phase),
    do: :gen_tcp.send(socket, iodata)

  defp send_stream_response(state, iodata, phase),
    do: ConnSend.send(state.socket, state.transport, iodata, phase, %{client_id: state.client_id})

  defp handle_sendfile_result({:sent, new_state}, _key, _state, _fn),
    do: {:continue, "", new_state}

  defp handle_sendfile_result({:error_after_header, _reason}, _key, state, _fn),
    do: {:quit, "", state}

  defp handle_sendfile_result(:fallback, key, state, dispatch_normal_fn),
    do: dispatch_normal_fn.("GET", [key], state)

  defp handle_file_stream_result({:sent, new_state}, _key, _state, _fn),
    do: {:continue, "", new_state}

  defp handle_file_stream_result({:error_after_header, _reason}, _key, state, _fn),
    do: {:quit, "", state}

  defp handle_file_stream_result(:fallback, key, state, dispatch_normal_fn),
    do: dispatch_normal_fn.("GET", [key], state)

  @doc false
  def stream_file_ref(key, path, offset, size, state) do
    stream_file_ref(key, key, path, offset, size, state)
  end

  defp stream_file_ref(key, validate_key, path, offset, size, state) do
    result = do_stream_file_ref_get(validate_key, path, offset, size, state)
    emit_file_stream_result(result, size, state)

    case result do
      {:sent, new_state, _chunks} ->
        {:sent, ConnTracking.maybe_track_read("GET", [key], :file_stream_ok, new_state)}

      :fallback ->
        :fallback

      {:error_after_header, reason, _chunks} ->
        {:error_after_header, reason}
    end
  end

  @doc false
  def send_file_ref_response(key, path, offset, size, %{transport: :ranch_tcp} = state),
    do: send_file_ref(key, key, path, offset, size, state)

  def send_file_ref_response(key, path, offset, size, state),
    do: stream_file_ref(key, key, path, offset, size, state)

  def send_file_ref_response(
        key,
        validate_key,
        path,
        offset,
        size,
        %{transport: :ranch_tcp} = state
      ),
      do: send_file_ref(key, validate_key, path, offset, size, state)

  def send_file_ref_response(key, validate_key, path, offset, size, state),
    do: stream_file_ref(key, validate_key, path, offset, size, state)

  @doc false
  def send_file_ref_element_response(key, path, offset, size, %{transport: :ranch_tcp} = state),
    do: send_file_ref_element(key, key, path, offset, size, state)

  def send_file_ref_element_response(key, path, offset, size, state) do
    result = do_stream_file_ref_get(key, path, offset, size, state)
    emit_file_stream_result(result, size, state)

    case result do
      {:sent, new_state, _chunks} -> {:sent, new_state}
      :fallback -> :fallback
      {:error_after_header, reason, _chunks} -> {:error_after_header, reason}
    end
  end

  def send_file_ref_element_response(
        key,
        validate_key,
        path,
        offset,
        size,
        %{transport: :ranch_tcp} = state
      ),
      do: send_file_ref_element(key, validate_key, path, offset, size, state)

  def send_file_ref_element_response(_key, validate_key, path, offset, size, state) do
    result = do_stream_file_ref_get(validate_key, path, offset, size, state)
    emit_file_stream_result(result, size, state)

    case result do
      {:sent, new_state, _chunks} -> {:sent, new_state}
      :fallback -> :fallback
      {:error_after_header, reason, _chunks} -> {:error_after_header, reason}
    end
  end

  defp do_stream_file_get(path, offset, size, validator, state) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case validate_open_file_range(path, fd, offset, size, validator) do
            :ok -> stream_file_get_open(fd, offset, size, state)
            :mismatch -> :fallback
          end
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :fallback
    end
  end

  defp do_stream_file_ref_get(key, path, offset, size, state) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case validate_open_file_ref(path, fd, key, offset, size) do
            :ok -> stream_file_get_open(fd, offset, size, state)
            :mismatch -> :fallback
          end
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :fallback
    end
  end

  defp stream_file_get_open(fd, offset, size, state) do
    header = [?$, Integer.to_string(size), "\r\n"]

    case ConnSend.send(state.socket, state.transport, header, :file_stream_header, %{
           client_id: state.client_id
         }) do
      :ok ->
        case stream_file_chunks(fd, offset, size, state, 0) do
          {:ok, chunks} ->
            case ConnSend.send(state.socket, state.transport, "\r\n", :file_stream_trailer, %{
                   client_id: state.client_id
                 }) do
              :ok -> {:sent, state, chunks}
              {:error, reason} -> {:error_after_header, reason, chunks}
            end

          {:error, reason, chunks} ->
            {:error_after_header, reason, chunks}
        end

      {:error, _reason} ->
        :fallback
    end
  end

  defp stream_file_chunks(_fd, _offset, 0, _state, chunks), do: {:ok, chunks}

  defp stream_file_chunks(fd, offset, remaining, state, chunks) do
    read_size = min(remaining, @file_stream_chunk_bytes)

    case :file.pread(fd, offset, read_size) do
      {:ok, data} when is_binary(data) and byte_size(data) > 0 ->
        sent = byte_size(data)

        case ConnSend.send(state.socket, state.transport, data, :file_stream_chunk, %{
               client_id: state.client_id
             }) do
          :ok -> stream_file_chunks(fd, offset + sent, remaining - sent, state, chunks + 1)
          {:error, reason} -> {:error, reason, chunks}
        end

      {:ok, _empty} ->
        {:error, :eof, chunks}

      :eof ->
        {:error, :eof, chunks}

      {:error, reason} ->
        {:error, reason, chunks}
    end
  end

  @doc false
  def send_file_ref(key, path, offset, size, state) do
    send_file_ref(key, key, path, offset, size, state)
  end

  defp send_file_ref(key, validate_key, path, offset, size, state) do
    result = do_sendfile_get(key, validate_key, path, offset, size, state, true)
    emit_sendfile_result(result, size, state)
    result
  end

  @doc false
  def send_file_ref_element(key, path, offset, size, state) do
    send_file_ref_element(key, key, path, offset, size, state)
  end

  defp send_file_ref_element(key, validate_key, path, offset, size, state) do
    result = do_sendfile_get(key, validate_key, path, offset, size, state, false)
    emit_sendfile_result(result, size, state)
    result
  end

  defp do_sendfile_get(key, validate_key, path, offset, size, state, track_read?) do
    socket = state.socket

    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          case validate_open_file_ref(path, fd, validate_key, offset, size) do
            :ok -> send_with_cork(socket, fd, offset, size, key, state, track_read?)
            :mismatch -> :fallback
          end
        after
          :file.close(fd)
        end

      {:error, _} ->
        :fallback
    end
  end

  defp send_with_cork(socket, fd, offset, size, key, state, track_read?) do
    header = [?$, Integer.to_string(size), "\r\n"]
    TcpOpts.set_cork(socket, true)

    case :gen_tcp.send(socket, header) do
      :ok ->
        result = send_file_and_trailer(socket, fd, offset, size, key, state, track_read?)
        TcpOpts.set_cork(socket, false)
        result

      {:error, _} ->
        TcpOpts.set_cork(socket, false)
        :fallback
    end
  end

  defp send_file_and_trailer(socket, fd, offset, size, key, state, track_read?) do
    case :file.sendfile(fd, socket, offset, size, []) do
      {:ok, ^size} ->
        case :gen_tcp.send(socket, "\r\n") do
          :ok ->
            new_state =
              if track_read? do
                ConnTracking.maybe_track_read("GET", [key], :sendfile_ok, state)
              else
                state
              end

            {:sent, new_state}

          {:error, reason} ->
            {:error_after_header, reason}
        end

      {:ok, sent} ->
        {:error_after_header, {:short_sendfile, sent, size}}

      {:error, reason} ->
        {:error_after_header, reason}
    end
  end

  defp validate_open_file_ref(path, fd, key, value_offset, value_size) do
    if blob_file_ref_path?(path) do
      validate_open_blob_file_ref(path, fd, value_offset, value_size)
    else
      validate_open_value_ref(fd, key, value_offset, value_size)
    end
  end

  defp blob_file_ref_path?(path) when is_binary(path),
    do: Path.extname(path) in [".blob", ".bloblog"]

  defp blob_file_ref_path?(_path), do: false

  defp validate_open_file_range(_path, _fd, _offset, _size, :none), do: :ok

  defp validate_open_file_range(path, fd, _offset, _size, {:blob, value_offset, value_size}),
    do: validate_open_blob_file_ref(path, fd, value_offset, value_size)

  defp validate_open_blob_file_ref(path, fd, value_offset, value_size)
       when is_integer(value_offset) and value_offset >= 0 and is_integer(value_size) and
              value_size >= 0 do
    case Path.extname(path) do
      ".blob" -> validate_open_legacy_blob_file_ref(path, fd, value_offset, value_size)
      ".bloblog" -> validate_open_segment_blob_file_ref(fd, value_offset, value_size)
      _other -> :mismatch
    end
  end

  defp validate_open_blob_file_ref(_path, _fd, _value_offset, _value_size), do: :mismatch

  defp validate_open_legacy_blob_file_ref(path, fd, value_offset, value_size) do
    with true <- value_offset == 0,
         {:ok, ^value_size} <- :file.position(fd, :eof),
         {:ok, expected_checksum} <- blob_checksum_from_path(path),
         {:ok, ^expected_checksum} <- hash_open_file(fd) do
      :ok
    else
      _ -> :mismatch
    end
  end

  defp validate_open_segment_blob_file_ref(fd, value_offset, value_size) do
    header_offset = value_offset - @blob_segment_header_bytes

    with true <- header_offset >= 0,
         {:ok, header} when byte_size(header) == @blob_segment_header_bytes <-
           :file.pread(fd, header_offset, @blob_segment_header_bytes),
         {:ok, ^value_size, expected_checksum} <- decode_blob_segment_header(header),
         {:ok, ^expected_checksum} <- hash_open_file_range(fd, value_offset, value_size) do
      :ok
    else
      _ -> :mismatch
    end
  end

  defp decode_blob_segment_header(
         <<@blob_segment_header_magic::binary, size::unsigned-big-64, checksum::binary-size(32)>>
       ),
       do: {:ok, size, checksum}

  defp decode_blob_segment_header(_header), do: :error

  defp hash_open_file(fd) do
    with {:ok, 0} <- :file.position(fd, 0) do
      hash_open_file(fd, :crypto.hash_init(:sha256))
    end
  end

  defp hash_open_file_range(_fd, _offset, 0),
    do: {:ok, :crypto.hash(:sha256, <<>>)}

  defp hash_open_file_range(fd, offset, size) do
    hash_open_file_range(fd, offset, size, :crypto.hash_init(:sha256))
  end

  defp hash_open_file_range(_fd, _offset, 0, hash_state),
    do: {:ok, :crypto.hash_final(hash_state)}

  defp hash_open_file_range(fd, offset, remaining, hash_state) do
    read_size = min(remaining, @file_stream_chunk_bytes)

    case :file.pread(fd, offset, read_size) do
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

  defp hash_open_file(fd, hash_state) do
    case :file.read(fd, @file_stream_chunk_bytes) do
      {:ok, chunk} when is_binary(chunk) and byte_size(chunk) > 0 ->
        hash_open_file(fd, :crypto.hash_update(hash_state, chunk))

      :eof ->
        {:ok, :crypto.hash_final(hash_state)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp validate_open_value_ref(fd, key, value_offset, value_size)
       when is_binary(key) and is_integer(value_offset) and is_integer(value_size) do
    key_size = byte_size(key)

    with true <- value_size <= @bitcask_tombstone_value_size,
         record_offset when record_offset >= 0 <-
           value_offset - @bitcask_header_size - key_size,
         {:ok, header} when byte_size(header) == @bitcask_header_size <-
           :file.pread(fd, record_offset, @bitcask_header_size),
         ^key_size <- decode_le_unsigned(header, 20, 2),
         ^value_size <- decode_le_unsigned(header, 22, 4),
         {:ok, ^key} <- :file.pread(fd, record_offset + @bitcask_header_size, key_size) do
      :ok
    else
      _ -> :mismatch
    end
  end

  defp validate_open_value_ref(_fd, _key, _value_offset, _value_size), do: :mismatch

  defp decode_le_unsigned(binary, offset, size) do
    binary
    |> binary_part(offset, size)
    |> :binary.decode_unsigned(:little)
  end

  defp emit_sendfile_result({:sent, _state}, size, state) do
    emit_sendfile(:ok, size, state, %{})
  end

  defp emit_sendfile_result(:fallback, size, state) do
    emit_sendfile(:fallback, size, state, %{})
  end

  defp emit_sendfile_result({:error_after_header, reason}, size, state) do
    emit_sendfile(:error_after_header, size, state, %{reason: reason})
  end

  defp emit_sendfile(result, size, state, metadata) do
    :telemetry.execute(
      [:ferricstore, :server, :sendfile],
      %{bytes: size},
      Map.merge(%{result: result, client_id: state.client_id}, metadata)
    )
  end

  defp emit_file_stream_result({:sent, _state, chunks}, size, state) do
    emit_file_stream(:ok, size, chunks, state, %{})
  end

  defp emit_file_stream_result(:fallback, size, state) do
    emit_file_stream(:fallback, size, 0, state, %{})
  end

  defp emit_file_stream_result({:error_after_header, reason, chunks}, size, state) do
    emit_file_stream(:error_after_header, size, chunks, state, %{reason: reason})
  end

  defp emit_file_stream(result, size, chunks, state, metadata) do
    :telemetry.execute(
      [:ferricstore, :server, :file_stream],
      %{bytes: size, chunks: chunks},
      Map.merge(
        %{result: result, client_id: state.client_id, transport: state.transport},
        metadata
      )
    )
  end

  defp in_pubsub_mode?(%{pubsub_channels: nil}), do: false

  defp in_pubsub_mode?(state),
    do: MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0

  defp namespace_keys(nil, keys), do: keys

  defp namespace_keys(namespace, keys) when is_binary(namespace),
    do: Enum.map(keys, &(namespace <> &1))

  defp namespace_key(nil, key), do: key
  defp namespace_key(namespace, key) when is_binary(namespace), do: namespace <> key
end
