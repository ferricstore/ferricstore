defmodule FerricstoreServer.Connection.Sendfile do
  @moduledoc "Large cold-value response streaming for GET/MGET/GETRANGE."

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Send, as: ConnSend
  alias FerricstoreServer.Connection.Sendfile.IO, as: SendIO
  alias FerricstoreServer.Connection.Sendfile.Stream, as: FileStream
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

    case Router.get_with_deferred_blob_file_ref(state.instance_ctx, lookup_key) do
      {:hot, value} ->
        encode_get_result(value, key, state)

      {:cold_ref, path, offset, size} when size >= @sendfile_threshold_bytes ->
        stream_large_get_ref(key, lookup_key, path, offset, size, state, dispatch_normal_fn)

      {:cold_ref, path, offset, size} ->
        case SendIO.pread_file_ref_value(lookup_key, path, offset, size) do
          {:ok, value} -> encode_get_result(value, key, state)
          :fallback -> dispatch_normal_fn.("GET", [key], state)
        end

      {:cold_value, value} ->
        encode_get_result(value, key, state)

      {:error, _reason} = error ->
        encode_get_result(error, key, state)

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
    {:ok,
     Router.batch_get_with_deferred_blob_file_refs(instance_ctx, lookup_keys, threshold_bytes())}
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
        case SendIO.normalize_byte_range(value_size, start_idx, end_idx) do
          :empty ->
            {:value, ""}

          {relative_offset, count} when count >= @sendfile_threshold_bytes ->
            validator = file_range_validator(path, value_offset, value_size)

            {:file_range, args, key, start_idx, end_idx, path, value_offset + relative_offset,
             count, validator}

          {relative_offset, count} ->
            case SendIO.pread_file_ref_range(path, value_offset, value_size, relative_offset, count) do
              {:ok, value} -> {:value, value}
              :fallback -> :fallback
            end
        end

      _other ->
        :fallback
    end
  end

  defp fetch_get_with_file_ref(instance_ctx, lookup_key) do
    {:ok, Router.get_with_deferred_blob_file_ref(instance_ctx, lookup_key)}
  catch
    _, _ -> :error
  end

  defp file_range_validator(path, value_offset, value_size) do
    if SendIO.blob_file_ref_path?(path), do: {:blob, value_offset, value_size}, else: :none
  end

  defp default_file_range_validator(path, offset, size) do
    if SendIO.blob_file_ref_path?(path), do: {:blob, offset, size}, else: :none
  end

  @doc false
  def materialize_getrange(key, start_idx, end_idx, state) do
    case Router.get(state.instance_ctx, namespace_key(state.sandbox_namespace, key)) do
      nil ->
        ""

      value when is_binary(value) ->
        SendIO.slice_value(value, start_idx, end_idx)

      value when is_integer(value) ->
        value |> Integer.to_string() |> SendIO.slice_value(start_idx, end_idx)

      value when is_float(value) ->
        value |> Float.to_string() |> SendIO.slice_value(start_idx, end_idx)
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
    SendIO.emit_sendfile_result(result, size, state)

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
    result = FileStream.do_stream_file_get(path, offset, size, validator, state)
    SendIO.emit_file_stream_result(result, size, state)

    case result do
      {:sent, new_state, _chunks} ->
        {:sent, ConnTracking.maybe_track_read("GETRANGE", args, :file_stream_ok, new_state)}

      :fallback ->
        :fallback

      {:error_after_header, reason, _chunks} ->
        {:error_after_header, reason}
    end
  end

  @doc false
  def send_file_range_response_cached(
        args,
        path,
        offset,
        size,
        validator,
        %{transport: :ranch_tcp} = state,
        file_cache
      ) do
    case SendIO.cached_file_open(path, file_cache) do
      {:ok, fd, new_cache} ->
        {result, new_cache} =
          case SendIO.cached_validate_file_range(
                 path,
                 fd,
                 offset,
                 size,
                 validator,
                 :sendfile,
                 new_cache
               ) do
            {:ok, validated_cache} ->
              {send_file_range_with_cork(state.socket, fd, offset, size, state), validated_cache}

            {:mismatch, validated_cache} ->
              {:fallback, validated_cache}
          end

        SendIO.emit_sendfile_result(result, size, state)

        case result do
          {:sent, new_state} ->
            tracked_state =
              ConnTracking.maybe_track_read("GETRANGE", args, :sendfile_ok, new_state)

            {:sent, tracked_state, new_cache}

          :fallback ->
            {:fallback, new_cache}

          {:error_after_header, reason} ->
            {:error_after_header, reason, new_cache}
        end

      {:error, _reason} ->
        {:fallback, file_cache}
    end
  end

  def send_file_range_response_cached(args, path, offset, size, validator, state, file_cache) do
    case SendIO.cached_file_open(path, file_cache) do
      {:ok, fd, new_cache} ->
        {result, new_cache} =
          case SendIO.cached_validate_file_range(
                 path,
                 fd,
                 offset,
                 size,
                 validator,
                 :file_stream,
                 new_cache
               ) do
            {:ok, validated_cache} ->
              {FileStream.stream_file_get_open(fd, offset, size, state), validated_cache}

            {:mismatch, validated_cache} ->
              {:fallback, validated_cache}
          end

        SendIO.emit_file_stream_result(result, size, state)

        case result do
          {:sent, new_state, _chunks} ->
            tracked_state =
              ConnTracking.maybe_track_read("GETRANGE", args, :file_stream_ok, new_state)

            {:sent, tracked_state, new_cache}

          :fallback ->
            {:fallback, new_cache}

          {:error_after_header, reason, _chunks} ->
            {:error_after_header, reason, new_cache}
        end

      {:error, _reason} ->
        {:fallback, file_cache}
    end
  end

  defp do_sendfile_range(path, offset, size, validator, state) do
    case SendIO.file_open(path) do
      {:ok, fd} ->
        try do
          do_sendfile_range_open(path, fd, offset, size, validator, state)
        after
          :file.close(fd)
        end

      {:error, _reason} ->
        :fallback
    end
  end

  defp do_sendfile_range_open(path, fd, offset, size, validator, state) do
    socket = state.socket

    case SendIO.validate_open_file_range(path, fd, offset, size, validator, :sendfile) do
      :ok -> send_file_range_with_cork(socket, fd, offset, size, state)
      :mismatch -> :fallback
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
    entries =
      keys
      |> Enum.zip(lookup_keys)
      |> Enum.zip(results)

    {result, file_cache} = stream_mget_entries(entries, state, SendIO.new_file_cache())
    SendIO.close_file_cache(file_cache)

    case result do
      {:sent, new_state} ->
        {:sent, ConnTracking.maybe_track_read("MGET", keys, :sendfile_ok, new_state)}

      other ->
        other
    end
  end

  defp stream_mget_entries([], state, file_cache), do: {{:sent, state}, file_cache}

  defp stream_mget_entries(
         [{{key, lookup_key}, {:file_ref, path, offset, size}} | rest],
         state,
         file_cache
       ) do
    case send_file_ref_element_response_cached(
           key,
           lookup_key,
           path,
           offset,
           size,
           state,
           file_cache
         ) do
      {:sent, new_state, new_cache} ->
        stream_mget_entries(rest, new_state, new_cache)

      {:fallback, new_cache} ->
        case send_stream_response(
               state,
               Encoder.encode(Router.get(state.instance_ctx, lookup_key)),
               :mget_stream_fallback
             ) do
          :ok -> stream_mget_entries(rest, state, new_cache)
          {:error, reason} -> {{:error_after_header, reason}, new_cache}
        end

      {:error_after_header, reason, new_cache} ->
        {{:error_after_header, reason}, new_cache}
    end
  end

  defp stream_mget_entries([{{_key, _lookup_key}, value} | rest], state, file_cache) do
    case send_stream_response(state, Encoder.encode(value), :mget_stream_element) do
      :ok -> stream_mget_entries(rest, state, file_cache)
      {:error, reason} -> {{:error_after_header, reason}, file_cache}
    end
  end

  @doc false
  def send_file_ref_response_cached(
        key,
        validate_key,
        path,
        offset,
        size,
        %{transport: :ranch_tcp} = state,
        file_cache
      ) do
    case SendIO.cached_file_open(path, file_cache) do
      {:ok, fd, new_cache} ->
        {result, new_cache} =
          case SendIO.cached_validate_file_ref(
                 path,
                 fd,
                 validate_key,
                 offset,
                 size,
                 :sendfile,
                 new_cache
               ) do
            {:ok, validated_cache} ->
              {send_with_cork(state.socket, fd, offset, size, key, state, true), validated_cache}

            {:mismatch, validated_cache} ->
              {:fallback, validated_cache}
          end

        SendIO.emit_sendfile_result(result, size, state)

        case result do
          {:sent, new_state} ->
            {:sent, new_state, new_cache}

          :fallback ->
            {:fallback, new_cache}

          {:error_after_header, reason} ->
            {:error_after_header, reason, new_cache}
        end

      {:error, _reason} ->
        {:fallback, file_cache}
    end
  end

  def send_file_ref_response_cached(
        key,
        validate_key,
        path,
        offset,
        size,
        state,
        file_cache
      ) do
    case SendIO.cached_file_open(path, file_cache) do
      {:ok, fd, new_cache} ->
        {result, new_cache} =
          case SendIO.cached_validate_file_ref(
                 path,
                 fd,
                 validate_key,
                 offset,
                 size,
                 :file_stream,
                 new_cache
               ) do
            {:ok, validated_cache} ->
              {FileStream.stream_file_get_open(fd, offset, size, state), validated_cache}

            {:mismatch, validated_cache} ->
              {:fallback, validated_cache}
          end

        SendIO.emit_file_stream_result(result, size, state)

        case result do
          {:sent, new_state, _chunks} ->
            tracked_state =
              ConnTracking.maybe_track_read("GET", [key], :file_stream_ok, new_state)

            {:sent, tracked_state, new_cache}

          :fallback ->
            {:fallback, new_cache}

          {:error_after_header, reason, _chunks} ->
            {:error_after_header, reason, new_cache}
        end

      {:error, _reason} ->
        {:fallback, file_cache}
    end
  end

  @doc false
  def send_file_ref_element_response_cached(
        key,
        validate_key,
        path,
        offset,
        size,
        %{transport: :ranch_tcp} = state,
        file_cache
      ) do
    case SendIO.cached_file_open(path, file_cache) do
      {:ok, fd, new_cache} ->
        {result, new_cache} =
          case SendIO.cached_validate_file_ref(
                 path,
                 fd,
                 validate_key,
                 offset,
                 size,
                 :sendfile,
                 new_cache
               ) do
            {:ok, validated_cache} ->
              {send_with_cork(state.socket, fd, offset, size, key, state, false), validated_cache}

            {:mismatch, validated_cache} ->
              {:fallback, validated_cache}
          end

        SendIO.emit_sendfile_result(result, size, state)

        case result do
          {:sent, new_state} ->
            {:sent, new_state, new_cache}

          :fallback ->
            {:fallback, new_cache}

          {:error_after_header, reason} ->
            {:error_after_header, reason, new_cache}
        end

      {:error, _reason} ->
        {:fallback, file_cache}
    end
  end

  def send_file_ref_element_response_cached(
        _key,
        validate_key,
        path,
        offset,
        size,
        state,
        file_cache
      ) do
    case SendIO.cached_file_open(path, file_cache) do
      {:ok, fd, new_cache} ->
        {result, new_cache} =
          case SendIO.cached_validate_file_ref(
                 path,
                 fd,
                 validate_key,
                 offset,
                 size,
                 :file_stream,
                 new_cache
               ) do
            {:ok, validated_cache} ->
              {FileStream.stream_file_get_open(fd, offset, size, state), validated_cache}

            {:mismatch, validated_cache} ->
              {:fallback, validated_cache}
          end

        SendIO.emit_file_stream_result(result, size, state)

        case result do
          {:sent, new_state, _chunks} ->
            {:sent, new_state, new_cache}

          :fallback ->
            {:fallback, new_cache}

          {:error_after_header, reason, _chunks} ->
            {:error_after_header, reason, new_cache}
        end

      {:error, _reason} ->
        {:fallback, file_cache}
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
    result = FileStream.do_stream_file_ref_get(validate_key, path, offset, size, state)
    SendIO.emit_file_stream_result(result, size, state)

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
    result = FileStream.do_stream_file_ref_get(key, path, offset, size, state)
    SendIO.emit_file_stream_result(result, size, state)

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
    result = FileStream.do_stream_file_ref_get(validate_key, path, offset, size, state)
    SendIO.emit_file_stream_result(result, size, state)

    case result do
      {:sent, new_state, _chunks} -> {:sent, new_state}
      :fallback -> :fallback
      {:error_after_header, reason, _chunks} -> {:error_after_header, reason}
    end
  end


  @doc false
  def send_file_ref(key, path, offset, size, state) do
    send_file_ref(key, key, path, offset, size, state)
  end

  defp send_file_ref(key, validate_key, path, offset, size, state) do
    result = do_sendfile_get(key, validate_key, path, offset, size, state, true)
    SendIO.emit_sendfile_result(result, size, state)
    result
  end

  @doc false
  def send_file_ref_element(key, path, offset, size, state) do
    send_file_ref_element(key, key, path, offset, size, state)
  end

  defp send_file_ref_element(key, validate_key, path, offset, size, state) do
    result = do_sendfile_get(key, validate_key, path, offset, size, state, false)
    SendIO.emit_sendfile_result(result, size, state)
    result
  end

  defp do_sendfile_get(key, validate_key, path, offset, size, state, track_read?) do
    case SendIO.file_open(path) do
      {:ok, fd} ->
        try do
          do_sendfile_get_open(key, validate_key, path, fd, offset, size, state, track_read?)
        after
          :file.close(fd)
        end

      {:error, _} ->
        :fallback
    end
  end

  defp do_sendfile_get_open(key, validate_key, path, fd, offset, size, state, track_read?) do
    socket = state.socket

    case SendIO.validate_open_file_ref(path, fd, validate_key, offset, size, :sendfile) do
      :ok -> send_with_cork(socket, fd, offset, size, key, state, track_read?)
      :mismatch -> :fallback
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

  @doc false
  def new_file_cache, do: SendIO.new_file_cache()

  @doc false
  def close_file_cache(file_cache), do: SendIO.close_file_cache(file_cache)

  defp in_pubsub_mode?(%{pubsub_channels: nil}), do: false

  defp in_pubsub_mode?(state),
    do: MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0

  defp namespace_keys(nil, keys), do: keys

  defp namespace_keys(namespace, keys) when is_binary(namespace),
    do: Enum.map(keys, &(namespace <> &1))

  defp namespace_key(nil, key), do: key
  defp namespace_key(namespace, key) when is_binary(namespace), do: namespace <> key
end
