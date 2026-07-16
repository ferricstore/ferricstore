defmodule FerricstoreServer.Native.Listener do
  @moduledoc """
  Ranch TCP listener for FerricStore's native binary protocol.

  This is the standalone FerricStore SDK data plane.
  """

  @listener_ref __MODULE__

  alias FerricstoreServer.Native.Connection.FrameBuffer

  @spec ref() :: atom()
  def ref, do: @listener_ref

  @spec port() :: :inet.port_number()
  def port do
    :ranch.get_port(@listener_ref)
  end

  @spec running?() :: boolean()
  def running? do
    match?({:ok, _}, try_port())
  end

  @spec max_connections() :: non_neg_integer()
  def max_connections do
    Application.get_env(:ferricstore, :maxclients, 10_000)
  end

  @doc false
  @spec socket_opts(:inet.port_number(), keyword()) :: keyword()
  def socket_opts(port, opts \\ []) do
    [
      port: port,
      nodelay: Keyword.get(opts, :nodelay, Application.get_env(:ferricstore, :tcp_nodelay, true)),
      recbuf: Application.get_env(:ferricstore, :tcp_recbuf, 131_072),
      sndbuf: Application.get_env(:ferricstore, :tcp_sndbuf, 131_072),
      backlog: Keyword.get(opts, :backlog, 1024),
      keepalive: true
    ] ++ send_timeout_opts()
  end

  @doc false
  @spec send_timeout_opts() :: keyword()
  def send_timeout_opts do
    timeout = Application.get_env(:ferricstore, :native_send_timeout_ms, 15_000)

    if is_integer(timeout) and timeout > 0 do
      [send_timeout: timeout, send_timeout_close: true]
    else
      raise ArgumentError, "native_send_timeout_ms must be a positive integer"
    end
  end

  @spec start(:inet.port_number(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start(port, opts \\ []) do
    transport_opts = %{
      max_connections: Keyword.get(opts, :max_connections, max_connections()),
      socket_opts: socket_opts(port, opts)
    }

    max_frame_bytes =
      opts
      |> Keyword.get(
        :max_frame_bytes,
        Application.get_env(:ferricstore, :native_max_frame_bytes, 16 * 1024 * 1024)
      )
      |> FrameBuffer.validate_max_frame_bytes!()

    protocol_opts = %{
      max_frame_bytes: max_frame_bytes,
      max_lanes:
        Keyword.get(
          opts,
          :max_lanes,
          Application.get_env(:ferricstore, :native_max_lanes_per_connection, 1024)
        ),
      lane_max_queue:
        Keyword.get(
          opts,
          :lane_max_queue,
          Application.get_env(:ferricstore, :native_lane_max_queue, 1024)
        )
    }

    :ranch.start_listener(
      @listener_ref,
      :ranch_tcp,
      transport_opts,
      FerricstoreServer.Native.Connection,
      protocol_opts
    )
  end

  @spec stop() :: :ok
  def stop do
    :ranch.stop_listener(@listener_ref)
  end

  defp try_port do
    {:ok, :ranch.get_port(@listener_ref)}
  rescue
    ArgumentError -> :error
  end
end
