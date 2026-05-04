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

  @spec threshold_bytes() :: pos_integer()
  def threshold_bytes, do: @sendfile_threshold_bytes

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
  Handles MGET over plain TCP without materializing large cold bulk elements.

  RESP arrays still need an array header, so this streams the header first, then
  sends normal encoded elements for nil/hot/small values and sendfile-backed
  bulk elements for large cold refs.
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
  Handles GETRANGE over plain TCP without materializing the full cold value.

  Large cold slices are streamed with sendfile. Smaller cold slices are read
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
        encode_get_result(value, lookup_key, state)

      {:cold_ref, path, offset, size} when size >= @sendfile_threshold_bytes ->
        stream_large_get_ref(key, path, offset, size, state, dispatch_normal_fn)

      {:cold_ref, _path, _offset, _size} ->
        dispatch_normal_fn.("GET", [key], state)

      {:cold_value, value} ->
        encode_get_result(value, lookup_key, state)

      :miss ->
        dispatch_normal_fn.("GET", [key], state)
    end
  end

  defp stream_large_get_ref(
         key,
         path,
         offset,
         size,
         %{transport: :ranch_tcp} = state,
         dispatch_normal_fn
       ) do
    handle_sendfile_result(
      send_file_ref(key, path, offset, size, state),
      key,
      state,
      dispatch_normal_fn
    )
  end

  defp stream_large_get_ref(key, path, offset, size, state, dispatch_normal_fn) do
    handle_file_stream_result(
      stream_file_ref(key, path, offset, size, state),
      key,
      state,
      dispatch_normal_fn
    )
  end

  defp encode_get_result(value, lookup_key, state) do
    new_state = ConnTracking.maybe_track_read("GET", [lookup_key], value, state)
    ConnTracking.maybe_notify_keyspace("GET", [lookup_key], value)
    ConnTracking.maybe_notify_tracking("GET", [lookup_key], value, state)
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
          dispatch_normal_fn.("MGET", keys, state)
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

      {:file_range, ^args, ^key, ^start_idx, ^end_idx, path, offset, count} ->
        handle_getrange_send_result(
          send_file_range(args, path, offset, count, state),
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
            {:file_range, args, key, start_idx, end_idx, path, value_offset + relative_offset,
             count}

          {relative_offset, count} ->
            case pread_file_range(path, value_offset + relative_offset, count) do
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
  def send_file_range(args, path, offset, size, state) do
    result = do_sendfile_range(path, offset, size, state)
    emit_sendfile_result(result, size, state)

    case result do
      {:sent, new_state} ->
        {:sent, ConnTracking.maybe_track_read("GETRANGE", args, :sendfile_ok, new_state)}

      other ->
        other
    end
  end

  defp do_sendfile_range(path, offset, size, state) do
    socket = state.socket

    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          send_file_range_with_cork(socket, fd, offset, size, state)
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

  defp send_mget_array(keys, lookup_keys, results, state) do
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

  defp stream_mget_elements(keys, lookup_keys, results, state) do
    keys
    |> Enum.zip(lookup_keys)
    |> Enum.zip(results)
    |> Enum.reduce_while({:sent, state}, fn
      {{key, lookup_key}, {:file_ref, path, offset, size}}, {:sent, acc_state} ->
        case send_file_ref_element(key, path, offset, size, acc_state) do
          {:sent, new_state} ->
            {:cont, {:sent, new_state}}

          :fallback ->
            case :gen_tcp.send(
                   acc_state.socket,
                   Encoder.encode(Router.get(acc_state.instance_ctx, lookup_key))
                 ) do
              :ok -> {:cont, {:sent, acc_state}}
              {:error, reason} -> {:halt, {:error_after_header, reason}}
            end

          {:error_after_header, _reason} = error ->
            {:halt, error}
        end

      {{_key, _lookup_key}, value}, {:sent, acc_state} ->
        case :gen_tcp.send(acc_state.socket, Encoder.encode(value)) do
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
    result = do_stream_file_get(path, offset, size, state)
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
    do: send_file_ref(key, path, offset, size, state)

  def send_file_ref_response(key, path, offset, size, state),
    do: stream_file_ref(key, path, offset, size, state)

  defp do_stream_file_get(path, offset, size, state) do
    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          stream_file_get_open(fd, offset, size, state)
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
    result = do_sendfile_get(key, path, offset, size, state, true)
    emit_sendfile_result(result, size, state)
    result
  end

  @doc false
  def send_file_ref_element(key, path, offset, size, state) do
    result = do_sendfile_get(key, path, offset, size, state, false)
    emit_sendfile_result(result, size, state)
    result
  end

  defp do_sendfile_get(key, path, offset, size, state, track_read?) do
    socket = state.socket

    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          send_with_cork(socket, fd, offset, size, key, state, track_read?)
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
