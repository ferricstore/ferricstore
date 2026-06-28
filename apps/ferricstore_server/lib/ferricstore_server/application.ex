defmodule FerricstoreServer.Application do
  @moduledoc """
  OTP Application for the FerricStore standalone server.

  Starts the network-facing children that expose the core engine:

    * Ferric native protocol TCP listener (`FerricstoreServer.Native.Listener`)
    * Ferric native protocol TLS listener (`FerricstoreServer.Native.TlsListener`) -- optional
    * HTTP health/metrics endpoint (`FerricstoreServer.Health.Endpoint`)

  This application depends on `:ferricstore` (the core engine). It injects
  server-specific callbacks (connected clients count, RSS tracking, server
  info) into the default Instance so the library can report them without
  knowing about the server.
  """

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    {:ok, _} = Application.ensure_all_started(:ranch)

    native_port = Application.get_env(:ferricstore, :native_port, 6388)
    health_port = Application.get_env(:ferricstore, :health_port, 4000)

    FerricstoreServer.Connection.Registry.init_table()

    children =
      [
        FerricstoreServer.Acl,
        pg_child_spec(),
        native_listener_spec(native_port)
      ] ++
        native_tls_listener_children() ++
        [FerricstoreServer.Health.Endpoint.child_spec(health_port)]

    opts = [
      strategy: :one_for_one,
      name: FerricstoreServer.Supervisor,
      max_restarts: 20,
      max_seconds: 10
    ]

    result = Supervisor.start_link(children, opts)

    inject_server_callbacks(native_port)

    result
  end

  @impl true
  def prep_stop(state) do
    Logger.info("FerricstoreServer: graceful shutdown starting")

    try do
      :ranch.suspend_listener(FerricstoreServer.Native.Listener)
      Logger.info("FerricstoreServer: protocol listener suspended (no new connections)")
    catch
      _, _ -> :ok
    end

    try do
      :ranch.suspend_listener(FerricstoreServer.Native.TlsListener)
      Logger.info("FerricstoreServer: protocol TLS listener suspended (no new connections)")
    catch
      _, _ -> :ok
    end

    grace_ms = Application.get_env(:ferricstore, :shutdown_grace_ms, 2_000)
    notify_native_goaway(grace_ms)

    active =
      listener_connection_count(FerricstoreServer.Native.Listener) +
        listener_connection_count(FerricstoreServer.Native.TlsListener)

    if active > 0 do
      Logger.info("FerricstoreServer: waiting #{grace_ms}ms for #{active} active connections")
      Process.sleep(grace_ms)
    end

    Logger.info("FerricstoreServer: graceful shutdown complete")
    state
  end

  defp inject_server_callbacks(native_port) do
    FerricStore.Instance.inject_callbacks(:default,
      connected_clients_fn: fn ->
        listener_connection_count(FerricstoreServer.Native.Listener) +
          listener_connection_count(FerricstoreServer.Native.TlsListener)
      end,
      process_rss_fn: &Ferricstore.MemoryGuard.process_rss_bytes/0,
      server_info_fn: fn ->
        %{
          protocol: "ferric",
          native_port: native_port
        }
        |> maybe_put_native_tls_port()
      end,
      raft_apply_hook: FerricstoreServer.RaftApplyHook.compose_current(:default)
    )
  end

  defp listener_connection_count(listener) do
    try do
      listener
      |> :ranch.procs(:connections)
      |> length()
    rescue
      _ -> 0
    catch
      _, _ -> 0
    end
  end

  defp native_connection_pids do
    native_listener_pids(FerricstoreServer.Native.Listener) ++
      native_listener_pids(FerricstoreServer.Native.TlsListener)
  end

  defp native_listener_pids(listener) do
    try do
      :ranch.procs(listener, :connections)
    rescue
      _ -> []
    catch
      _, _ -> []
    end
  end

  defp notify_native_goaway(grace_ms) do
    payload = %{
      reason: "server_shutdown",
      grace_ms: grace_ms,
      reconnect: true
    }

    Enum.each(native_connection_pids(), &send(&1, {:native_goaway, payload}))
  end

  defp maybe_put_native_tls_port(info) do
    case Application.get_env(:ferricstore, :native_tls_port) do
      nil -> info
      port -> Map.put(info, :native_tls_port, port)
    end
  end

  defp pg_child_spec do
    %{
      id: FerricstoreServer.PG,
      start: {:pg, :start_link, [FerricstoreServer.Connection.Auth.acl_pg_group()]}
    }
  end

  defp native_tls_listener_children do
    tls_opts = [
      port: Application.get_env(:ferricstore, :native_tls_port),
      certfile: Application.get_env(:ferricstore, :native_tls_cert_file),
      keyfile: Application.get_env(:ferricstore, :native_tls_key_file),
      cacertfile: Application.get_env(:ferricstore, :native_tls_ca_cert_file)
    ]

    case FerricstoreServer.Native.TlsListener.child_spec_if_configured(tls_opts) do
      nil -> []
      spec -> [spec]
    end
  end

  defp native_listener_spec(port) do
    nodelay = Application.get_env(:ferricstore, :tcp_nodelay, true)

    transport_opts = %{
      socket_opts: [
        port: port,
        nodelay: nodelay,
        recbuf: Application.get_env(:ferricstore, :tcp_recbuf, 131_072),
        sndbuf: Application.get_env(:ferricstore, :tcp_sndbuf, 131_072),
        backlog: 1024,
        keepalive: true
      ]
    }

    protocol_opts = %{
      max_frame_bytes:
        Application.get_env(:ferricstore, :native_max_frame_bytes, 16 * 1024 * 1024),
      max_lanes: Application.get_env(:ferricstore, :native_max_lanes_per_connection, 1024),
      lane_max_queue: Application.get_env(:ferricstore, :native_lane_max_queue, 1024)
    }

    :ranch.child_spec(
      FerricstoreServer.Native.Listener,
      :ranch_tcp,
      transport_opts,
      FerricstoreServer.Native.Connection,
      protocol_opts
    )
  end
end
