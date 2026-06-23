defmodule FerricstoreServer.Native.Listener do
  @moduledoc """
  Ranch TCP listener for FerricStore's native binary protocol.

  This is the standalone FerricStore SDK data plane.
  """

  @listener_ref __MODULE__

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

  @spec start(:inet.port_number(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start(port, opts \\ []) do
    transport_opts = %{
      socket_opts: [
        port: port,
        nodelay: Application.get_env(:ferricstore, :tcp_nodelay, true),
        recbuf: Application.get_env(:ferricstore, :tcp_recbuf, 131_072),
        sndbuf: Application.get_env(:ferricstore, :tcp_sndbuf, 131_072),
        backlog: Keyword.get(opts, :backlog, 1024),
        keepalive: true
      ]
    }

    protocol_opts = %{
      max_frame_bytes:
        Keyword.get(
          opts,
          :max_frame_bytes,
          Application.get_env(:ferricstore, :native_max_frame_bytes, 16 * 1024 * 1024)
        ),
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
