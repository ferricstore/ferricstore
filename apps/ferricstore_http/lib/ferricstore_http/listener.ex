defmodule FerricstoreHttp.Listener do
  @moduledoc false

  use GenServer

  alias FerricstoreHttp.{Config, Router}

  @listener_ref __MODULE__

  @spec start_link(Config.t()) :: GenServer.on_start()
  def start_link(%Config{} = config) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  @spec port() :: :inet.port_number()
  def port, do: :ranch.get_port(@listener_ref)

  @spec suspend() :: :ok
  def suspend do
    case :ranch.get_status(@listener_ref) do
      :running -> :ranch.suspend_listener(@listener_ref)
      :suspended -> :ok
    end
  rescue
    _not_running -> :ok
  catch
    :exit, _not_running -> :ok
  end

  @impl GenServer
  def init(%Config{} = config) do
    Process.flag(:trap_exit, true)

    case start_listener(config) do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> {:stop, reason}
    end
  end

  defp start_listener(%Config{tls: %{enabled: true} = tls} = config) do
    transport_options = %{
      max_connections: config.max_connections,
      num_acceptors: config.acceptors,
      socket_opts:
        socket_options(config) ++
          [certfile: tls.certfile, keyfile: tls.keyfile, versions: [:"tlsv1.2", :"tlsv1.3"]]
    }

    :cowboy.start_tls(@listener_ref, transport_options, protocol_options(config))
  end

  defp start_listener(%Config{} = config) do
    transport_options = %{
      max_connections: config.max_connections,
      num_acceptors: config.acceptors,
      socket_opts: socket_options(config)
    }

    :cowboy.start_clear(@listener_ref, transport_options, protocol_options(config))
  end

  defp socket_options(config) do
    [ip: config.ip, port: config.port, nodelay: true, keepalive: true]
  end

  defp protocol_options(config) do
    %{
      env: %{dispatch: Router.dispatch(config)},
      protocols: protocols(config),
      idle_timeout: config.idle_timeout_ms,
      request_timeout: config.request_header_timeout_ms,
      max_keepalive: config.max_keepalive_requests,
      max_headers: config.max_headers,
      max_header_name_length: config.max_header_name_bytes,
      max_header_value_length: config.max_header_value_bytes,
      max_request_line_length: config.max_request_line_bytes,
      ferricstore_max_header_name_bytes: config.max_header_name_bytes,
      ferricstore_max_header_value_bytes: config.max_header_value_bytes,
      ferricstore_max_request_line_bytes: config.max_request_line_bytes,
      stream_handlers: stream_handlers(config)
    }
    |> put_metrics_options(config)
  end

  defp put_metrics_options(options, %Config{metrics_enabled: false}), do: options

  defp put_metrics_options(options, %Config{metrics_enabled: true}) do
    Map.merge(options, %{
      metrics_callback: &FerricstoreHttp.Metrics.observe/1,
      metrics_req_filter: &Map.take(&1, [:method]),
      metrics_resp_headers_filter: fn _headers -> %{} end
    })
  end

  defp stream_handlers(%Config{metrics_enabled: true}) do
    [:cowboy_metrics_h, FerricstoreHttp.Admission.StreamHandler, :cowboy_stream_h]
  end

  defp stream_handlers(%Config{metrics_enabled: false}) do
    [FerricstoreHttp.Admission.StreamHandler, :cowboy_stream_h]
  end

  defp protocols(%Config{http2_enabled: true}), do: [:http, :http2]
  defp protocols(%Config{http2_enabled: false}), do: [:http]

  @impl GenServer
  def terminate(_reason, _state) do
    :cowboy.stop_listener(@listener_ref)
    :ok
  end
end
