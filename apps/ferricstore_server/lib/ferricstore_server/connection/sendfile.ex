defmodule FerricstoreServer.Connection.Sendfile do
  @moduledoc "Zero-copy sendfile optimization for large GET responses over ranch_tcp."

  alias FerricstoreServer.Resp.Encoder
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.TcpOpts
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking

  @sendfile_threshold_bytes Application.compile_env(
                              :ferricstore_server,
                              :sendfile_threshold,
                              65_536
                            )

  @spec threshold_bytes() :: pos_integer()
  def threshold_bytes, do: @sendfile_threshold_bytes

  @doc """
  Handles the GET command with sendfile optimization for `:ranch_tcp` transport.
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
        handle_sendfile_result(
          send_file_ref(key, path, offset, size, state),
          key,
          state,
          dispatch_normal_fn
        )

      {:cold_ref, _path, _offset, _size} ->
        dispatch_normal_fn.("GET", [key], state)

      {:cold_value, value} ->
        encode_get_result(value, lookup_key, state)

      :miss ->
        dispatch_normal_fn.("GET", [key], state)
    end
  end

  defp encode_get_result(value, lookup_key, state) do
    new_state = ConnTracking.maybe_track_read("GET", [lookup_key], value, state)
    ConnTracking.maybe_notify_keyspace("GET", [lookup_key], value)
    ConnTracking.maybe_notify_tracking("GET", [lookup_key], value, state)
    {:continue, Encoder.encode(value), new_state}
  end

  defp handle_sendfile_result({:sent, new_state}, _key, _state, _fn),
    do: {:continue, "", new_state}

  defp handle_sendfile_result({:error_after_header, _reason}, _key, state, _fn),
    do: {:quit, "", state}

  defp handle_sendfile_result(:fallback, key, state, dispatch_normal_fn),
    do: dispatch_normal_fn.("GET", [key], state)

  @doc false
  def send_file_ref(key, path, offset, size, state) do
    result = do_sendfile_get(key, path, offset, size, state)
    emit_sendfile_result(result, size, state)
    result
  end

  defp do_sendfile_get(key, path, offset, size, state) do
    socket = state.socket

    case :file.open(path, [:read, :raw, :binary]) do
      {:ok, fd} ->
        try do
          send_with_cork(socket, fd, offset, size, key, state)
        after
          :file.close(fd)
        end

      {:error, _} ->
        :fallback
    end
  end

  defp send_with_cork(socket, fd, offset, size, key, state) do
    header = [?$, Integer.to_string(size), "\r\n"]
    TcpOpts.set_cork(socket, true)

    case :gen_tcp.send(socket, header) do
      :ok ->
        result = send_file_and_trailer(socket, fd, offset, size, key, state)
        TcpOpts.set_cork(socket, false)
        result

      {:error, _} ->
        TcpOpts.set_cork(socket, false)
        :fallback
    end
  end

  defp send_file_and_trailer(socket, fd, offset, size, key, state) do
    case :file.sendfile(fd, socket, offset, size, []) do
      {:ok, ^size} ->
        case :gen_tcp.send(socket, "\r\n") do
          :ok ->
            new_state = ConnTracking.maybe_track_read("GET", [key], :sendfile_ok, state)
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

  defp in_pubsub_mode?(%{pubsub_channels: nil}), do: false

  defp in_pubsub_mode?(state),
    do: MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0
end
