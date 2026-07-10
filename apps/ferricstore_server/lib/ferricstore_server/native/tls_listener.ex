defmodule FerricstoreServer.Native.TlsListener do
  @moduledoc """
  Ranch TLS listener for FerricStore's native binary protocol.

  This keeps the native SDK data plane secure without forcing clients through
  a text-protocol TLS listener.
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
    try do
      _ = :ranch.get_port(@listener_ref)
      true
    rescue
      _ -> false
    catch
      :exit, _ -> false
    end
  end

  @spec start(keyword()) :: {:ok, pid()} | {:error, term()}
  def start(opts) do
    port = Keyword.fetch!(opts, :port)
    certfile = Keyword.fetch!(opts, :certfile)
    keyfile = Keyword.fetch!(opts, :keyfile)

    socket_opts =
      [
        port: port,
        certfile: certfile,
        keyfile: keyfile,
        versions: [:"tlsv1.3", :"tlsv1.2"]
      ] ++ ca_opt(Keyword.get(opts, :cacertfile))

    transport_opts = %{socket_opts: socket_opts, num_acceptors: 10}
    protocol_opts = native_protocol_opts()

    :ranch.start_listener(
      @listener_ref,
      :ranch_ssl,
      transport_opts,
      FerricstoreServer.Native.Connection,
      protocol_opts
    )
  end

  @spec stop() :: :ok
  def stop do
    :ranch.stop_listener(@listener_ref)
  end

  @spec child_spec_if_configured(keyword()) :: map() | nil
  def child_spec_if_configured(opts) do
    port = Keyword.get(opts, :port)
    certfile = Keyword.get(opts, :certfile)
    keyfile = Keyword.get(opts, :keyfile)

    if port && certfile && keyfile do
      socket_opts =
        [
          port: port,
          certfile: certfile,
          keyfile: keyfile,
          versions: [:"tlsv1.3", :"tlsv1.2"]
        ] ++ ca_opt(Keyword.get(opts, :cacertfile))

      transport_opts = %{socket_opts: socket_opts, num_acceptors: 10}

      :ranch.child_spec(
        @listener_ref,
        :ranch_ssl,
        transport_opts,
        FerricstoreServer.Native.Connection,
        native_protocol_opts()
      )
    end
  end

  defp native_protocol_opts do
    %{
      max_frame_bytes:
        :ferricstore
        |> Application.get_env(:native_max_frame_bytes, 16 * 1024 * 1024)
        |> FrameBuffer.validate_max_frame_bytes!(),
      max_lanes: Application.get_env(:ferricstore, :native_max_lanes_per_connection, 1024),
      lane_max_queue: Application.get_env(:ferricstore, :native_lane_max_queue, 1024)
    }
  end

  defp ca_opt(nil), do: []
  defp ca_opt(path), do: [cacertfile: path, verify: :verify_peer]
end
