defmodule FerricstoreServer.Application do
  @moduledoc """
  OTP Application for the FerricStore standalone server.

  Starts the network-facing children that expose the core engine over TCP:

    * Ranch TCP listener (`FerricstoreServer.Listener`)
    * Ranch TLS listener (`FerricstoreServer.TlsListener`) -- optional
    * HTTP health/metrics endpoint (`FerricstoreServer.Health.Endpoint`)

  This application depends on `:ferricstore` (the core engine). It injects
  server-specific callbacks (connected clients count, RSS tracking, server
  info) into the default Instance so the library can report them without
  knowing about the server.

  ## Supervision tree (`:one_for_one`)

  ```
  FerricstoreServer.Supervisor
  ├── :pg scope (FerricstoreServer.PG) — ACL invalidation process groups
  ├── Ranch listener (FerricstoreServer.Listener)
  ├── Ranch TLS listener (FerricstoreServer.TlsListener) [optional]
  └── Health HTTP endpoint (FerricstoreServer.Health.Endpoint)
  ```
  """

  use Application

  require Logger

  @impl true
  def start(_type, _args) do
    port = Application.get_env(:ferricstore, :port, 6379)
    health_port = Application.get_env(:ferricstore, :health_port, 4000)

    # Initialize ClientTracking ETS tables before any connection starts.
    FerricstoreServer.ClientTracking.init_tables()
    FerricstoreServer.Connection.Registry.init_table()

    children =
      [
        # ACL GenServer — manages user accounts, authentication, permissions.
        # Must start before Ranch listener so connections can authenticate.
        FerricstoreServer.Acl,
        # Start :pg scope for ACL invalidation broadcasts before the Ranch
        # listener so it's ready when the first connection process starts.
        pg_child_spec()
      ] ++
      [ranch_listener_spec(port)] ++
        tls_listener_children() ++
        [FerricstoreServer.Health.Endpoint.child_spec(health_port)]

    opts = [strategy: :one_for_one, name: FerricstoreServer.Supervisor, max_restarts: 20, max_seconds: 10]
    result = Supervisor.start_link(children, opts)

    # Inject server-specific callbacks into the default Instance.
    # The library uses these to report connected clients, RSS, etc.
    # without needing to know about the server app.
    inject_server_callbacks(port)

    result
  end

  @impl true
  def prep_stop(state) do
    Logger.info("FerricstoreServer: graceful shutdown starting")

    # Step 1: Stop accepting new connections.
    # Suspending the listener rejects new TCP connections while existing
    # ones continue to be served.
    try do
      :ranch.suspend_listener(FerricstoreServer.Listener)
      Logger.info("FerricstoreServer: listener suspended (no new connections)")
    catch
      _, _ -> :ok
    end

    # Step 2: Give active connections a grace period to finish in-flight
    # commands. Connections that are idle will be closed by the supervisor.
    # Connections mid-command get time to complete.
    grace_ms = Application.get_env(:ferricstore, :shutdown_grace_ms, 2_000)
    active = try do
      :ranch.procs(FerricstoreServer.Listener, :connections) |> length()
    catch
      _, _ -> 0
    end

    if active > 0 do
      Logger.info("FerricstoreServer: waiting #{grace_ms}ms for #{active} active connections")
      Process.sleep(grace_ms)
    end

    Logger.info("FerricstoreServer: graceful shutdown complete")
    state
  end

  # ---------------------------------------------------------------------------
  # Server callback injection
  # ---------------------------------------------------------------------------

  defp inject_server_callbacks(port) do
    FerricStore.Instance.inject_callbacks(:default,
      connected_clients_fn: fn ->
        try do
          :ranch.procs(FerricstoreServer.Listener, :connections) |> length()
        rescue
          _ -> 0
        end
      end,
      process_rss_fn: &Ferricstore.MemoryGuard.process_rss_bytes/0,
      server_info_fn: fn ->
        %{
          tcp_port: port,
          redis_mode: "standalone"
        }
      end,
      raft_apply_hook: &FerricstoreServer.Acl.handle_raft_command/1
    )
  end

  # ---------------------------------------------------------------------------
  # :pg scope for ACL cache invalidation
  # ---------------------------------------------------------------------------

  # Returns a child spec for the :pg scope used by connection processes to
  # receive ACL invalidation broadcasts. The scope name matches the constant
  # in FerricstoreServer.Connection.
  defp pg_child_spec do
    %{
      id: FerricstoreServer.PG,
      start: {:pg, :start_link, [FerricstoreServer.Connection.acl_pg_group()]}
    }
  end

  # ---------------------------------------------------------------------------
  # TLS listener children
  # ---------------------------------------------------------------------------

  # Returns a list with the TLS Ranch listener child spec if TLS is configured,
  # or an empty list otherwise.
  defp tls_listener_children do
    tls_opts = [
      port: Application.get_env(:ferricstore, :tls_port),
      certfile: Application.get_env(:ferricstore, :tls_cert_file),
      keyfile: Application.get_env(:ferricstore, :tls_key_file),
      cacertfile: Application.get_env(:ferricstore, :tls_ca_cert_file)
    ]

    case FerricstoreServer.TlsListener.child_spec_if_configured(tls_opts) do
      nil -> []
      spec -> [spec]
    end
  end

  # ---------------------------------------------------------------------------
  # Ranch child spec
  # ---------------------------------------------------------------------------

  # Builds a Ranch TCP listener child spec.
  #
  # Ranch 2.x exposes `:ranch.child_spec/5` which returns a plain Supervisor
  # child spec suitable for embedding in any supervisor.  The listener will
  # bind to `port` (0 = ephemeral, useful in tests).
  defp ranch_listener_spec(port) do
    nodelay = Application.get_env(:ferricstore, :tcp_nodelay, true)
    transport_opts = %{socket_opts: [
      port: port,
      nodelay: nodelay,
      recbuf: Application.get_env(:ferricstore, :tcp_recbuf, 131_072),
      sndbuf: Application.get_env(:ferricstore, :tcp_sndbuf, 131_072),
      backlog: 1024,
      keepalive: true
    ]}
    protocol_opts = %{}

    :ranch.child_spec(
      FerricstoreServer.Listener,
      :ranch_tcp,
      transport_opts,
      FerricstoreServer.Connection,
      protocol_opts
    )
  end
end
