defmodule FerricStore.SDK.Native.Connection do
  @moduledoc false

  use GenServer

  alias FerricStore.SDK.Native.Codec

  @default_timeout 5_000
  @max_frame_bytes 16 * 1024 * 1024

  defstruct [:socket, :transport, :endpoint, next_request_id: 1, buffer: ""]

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(endpoint), do: GenServer.start_link(__MODULE__, endpoint)

  @spec start(map()) :: GenServer.on_start()
  def start(endpoint), do: GenServer.start(__MODULE__, endpoint)

  @spec request(pid(), non_neg_integer(), map(), non_neg_integer(), timeout()) ::
          {:ok, term()} | {:error, term()}
  def request(pid, opcode, payload, lane_id, timeout \\ @default_timeout) do
    GenServer.call(pid, {:request, opcode, payload, lane_id, timeout}, timeout + 1_000)
  end

  @impl true
  def init(endpoint) do
    with {:ok, transport, socket} <- connect(endpoint) do
      {:ok, %__MODULE__{socket: socket, transport: transport, endpoint: endpoint}}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:request, opcode, payload, lane_id, timeout}, _from, state) do
    request_id = state.next_request_id
    frame = Codec.encode_frame(opcode, lane_id, request_id, payload)

    with :ok <- apply(state.transport, :send, [state.socket, frame]),
         {:ok, value, next_state} <- await_response(state, opcode, request_id, timeout) do
      {:reply, {:ok, value}, %{next_state | next_request_id: request_id + 1}}
    else
      {:error, reason, next_state} ->
        next_state = %{next_state | next_request_id: request_id + 1}

        if connection_failure?(reason) do
          {:stop, :normal, {:error, reason}, next_state}
        else
          {:reply, {:error, reason}, next_state}
        end

      {:error, reason} ->
        if connection_failure?(reason) do
          {:stop, :normal, {:error, reason}, state}
        else
          {:reply, {:error, reason}, state}
        end

      {status, value} when status in [:auth, :noperm, :busy, :reroute, :bad_request] ->
        {:reply, {:error, {status, value}}, state}
    end
  end

  defp connect(%{tls: true} = endpoint) do
    host = String.to_charlist(endpoint.host)
    port = Map.get(endpoint, :native_tls_port) || endpoint.native_port

    case :ssl.connect(
           host,
           port,
           tls_options(endpoint),
           endpoint[:connect_timeout] || @default_timeout
         ) do
      {:ok, socket} -> {:ok, :ssl, socket}
      {:error, reason} -> {:error, reason}
    end
  end

  defp connect(endpoint) do
    host = String.to_charlist(endpoint.host)
    opts = [:binary, active: false, packet: :raw, nodelay: true]

    case :gen_tcp.connect(
           host,
           endpoint.native_port,
           opts,
           endpoint[:connect_timeout] || @default_timeout
         ) do
      {:ok, socket} -> {:ok, :gen_tcp, socket}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @spec tls_options(map()) :: keyword()
  def tls_options(endpoint) do
    base = [mode: :binary, active: false, packet: :raw]

    if tls_verify?(endpoint) do
      host = Map.get(endpoint, :server_name) || endpoint.host

      base
      |> Keyword.merge(
        verify: :verify_peer,
        server_name_indication: String.to_charlist(host),
        customize_hostname_check: [
          match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
        ]
      )
      |> put_ca_options(endpoint)
    else
      Keyword.merge(base, verify: :verify_none)
    end
  end

  defp await_response(state, opcode, request_id, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await_response(state, opcode, request_id, deadline)
  end

  defp do_await_response(state, opcode, request_id, deadline) do
    case next_matching_response(state.buffer, opcode, request_id) do
      {:ok, value, rest} ->
        {:ok, value, %{state | buffer: rest}}

      {:error, reason, rest} ->
        {:error, reason, %{state | buffer: rest}}

      {:need_more, rest} ->
        remaining = deadline - System.monotonic_time(:millisecond)
        next_state = %{state | buffer: rest}

        if remaining <= 0 do
          {:error, :timeout}
        else
          case apply(next_state.transport, :recv, [next_state.socket, 0, remaining]) do
            {:ok, bytes} ->
              do_await_response(
                %{next_state | buffer: next_state.buffer <> bytes},
                opcode,
                request_id,
                deadline
              )

            {:error, reason} ->
              {:error, reason}
          end
        end
    end
  end

  defp next_matching_response(buffer, opcode, request_id) do
    case Codec.decode_frames(buffer, @max_frame_bytes) do
      {:ok, [], _rest} ->
        {:need_more, buffer}

      {:ok, frames, rest} ->
        case Enum.find(frames, fn {_lane, frame_opcode, frame_request_id, _flags, _body, _raw} ->
               frame_opcode == opcode and frame_request_id == request_id
             end) do
          nil ->
            {:need_more, buffer}

          {_lane, ^opcode, ^request_id, flags, body, _raw} = matched ->
            rest = preserve_unmatched_frames(frames, matched, rest)

            case Codec.decode_response(opcode, flags, body) do
              {:ok, value} -> {:ok, value, rest}
              {:error, reason} -> {:error, reason, rest}
              {status, value} -> {:error, {status, value}, rest}
            end
        end

      {:error, reason} ->
        {:error, reason, ""}
    end
  end

  @impl true
  def terminate(_reason, state) do
    close_socket(state)
    :ok
  end

  defp preserve_unmatched_frames(frames, matched, rest) do
    frames
    |> Enum.reject(&(&1 == matched))
    |> Enum.map(fn {_lane, _opcode, _request_id, _flags, _body, raw} -> raw end)
    |> then(&IO.iodata_to_binary([&1, rest]))
  end

  defp tls_verify?(endpoint) do
    not (Map.get(endpoint, :verify) == false or Map.get(endpoint, "verify") == false or
           Map.get(endpoint, :tls_verify) == false or Map.get(endpoint, "tls_verify") == false)
  end

  defp put_ca_options(opts, endpoint) do
    cond do
      cacertfile = Map.get(endpoint, :cacertfile) || Map.get(endpoint, "cacertfile") ->
        Keyword.put(opts, :cacertfile, cacertfile)

      cacerts = Map.get(endpoint, :cacerts) || Map.get(endpoint, "cacerts") ->
        Keyword.put(opts, :cacerts, cacerts)

      function_exported?(:public_key, :cacerts_get, 0) ->
        Keyword.put(opts, :cacerts, :public_key.cacerts_get())

      true ->
        opts
    end
  end

  defp connection_failure?(reason)
       when reason in [:closed, :econnreset, :econnrefused, :enetdown],
       do: true

  defp connection_failure?({:tls_alert, _alert}), do: true
  defp connection_failure?(_reason), do: false

  defp close_socket(%{socket: nil}), do: :ok

  defp close_socket(%{transport: transport, socket: socket}) do
    apply(transport, :close, [socket])
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end
end
