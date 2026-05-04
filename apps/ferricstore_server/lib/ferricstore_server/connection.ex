defmodule FerricstoreServer.Connection do
  @moduledoc """
  Ranch protocol handler for a single FerricStore client connection.

  Each accepted TCP connection spawns one `Connection` process. The process:

  1. Performs the `CLIENT HELLO 3` handshake (RESP3-only; rejects RESP2).
  2. Enters a receive loop, accumulating TCP chunks into a binary buffer.
  3. Parses all complete RESP3 frames from the buffer via `FerricstoreServer.Resp.Parser`.
  4. Dispatches commands using a **sliding window pipeline** (spec section 2C.2):
     - All "pure" commands (those that don't mutate connection state) in a
       pipeline batch are dispatched concurrently as `Task`s.
     - Responses are sent over the socket in-order: response N is sent as
       soon as responses 0..N are all complete. This means fast commands
       before a slow command get their responses delivered immediately,
       without waiting for the slow command to finish.
     - Stateful commands (MULTI, AUTH, SUBSCRIBE, blocking ops, etc.) act
       as barriers: all prior concurrent tasks are awaited and flushed
       before the stateful command executes synchronously.
  5. Handles `QUIT` (send `+OK`, close) and `RESET` (send `+RESET`, reset state).
  6. Closes cleanly on TCP EOF or any transport error.

  ## Transaction support (MULTI/EXEC/DISCARD/WATCH)

  Transactions are connection-level state. When `MULTI` is issued, the connection
  enters `:queuing` mode. Subsequent commands (except EXEC, DISCARD, MULTI, WATCH,
  UNWATCH) are queued instead of executed, returning `+QUEUED` to the client.

  `EXEC` executes all queued commands sequentially and returns an array of results.
  If `WATCH` was used and any watched key's shard write-version changed, `EXEC`
  returns nil (transaction aborted).

  `DISCARD` clears the queue and watched keys, returning to normal mode.

  ## Ranch protocol contract

  Ranch requires the protocol module to export `start_link/3` and the started
  process to call `:ranch.handshake/1` before reading from the socket.

  ## BEAM scheduler notes (active: N mode)

  The socket operates in `active: N` mode (default N=100): the kernel
  delivers exactly one `{:tcp, socket, data}` message to the process, then
  automatically switches the socket to passive. The process re-arms via
  delivers N messages then sends `{:tcp_passive, socket}`, at which point
  we re-arm with `transport.setopts(socket, active: N)`.

  This is superior to both `active: false` (blocking recv) and `active: true`
  (unbounded mailbox flooding):
  - The process can handle OTHER messages (waiter notifications, pub/sub pushes,
    client tracking invalidations) between TCP reads.
  - The BEAM scheduler can schedule other processes while waiting for TCP data.
  - Sliding window responses can be sent incrementally.
  - No risk of mailbox flooding (unlike `active: true`) since at most N messages
    is delivered at a time.
  """

  @behaviour :ranch_protocol

  alias Ferricstore.AuditLog
  alias FerricstoreServer.ClientTracking
  alias Ferricstore.Commands.Dispatcher
  alias FerricstoreServer.Resp.{Encoder, Parser}
  alias Ferricstore.Stats
  alias Ferricstore.Store.Router
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.Blocking, as: ConnBlocking
  alias FerricstoreServer.Connection.Pipeline, as: ConnPipeline
  alias FerricstoreServer.Connection.PubSub, as: ConnPubSub
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Connection.Send, as: ConnSend
  alias FerricstoreServer.Connection.Sendfile, as: ConnSendfile
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking
  alias FerricstoreServer.Connection.Transaction, as: ConnTransaction

  alias Ferricstore.PubSub, as: PS

  # Connection safety limits -- prevent unbounded memory growth per connection.
  # Maximum receive buffer size before the connection is closed (128 MB).
  @max_buffer_size 134_217_728

  # Connection state
  defstruct [
    :socket,
    :transport,
    :client_id,
    :client_name,
    :created_at,
    :peer,
    :instance_ctx,
    :stats_counter,
    buffer: "",
    multi_state: :none,
    multi_queue: [],
    multi_queue_count: 0,
    watched_keys: %{},
    authenticated: false,
    require_auth: false,
    username: "default",
    sandbox_namespace: nil,
    pubsub_channels: nil,
    pubsub_patterns: nil,
    tracking: nil,
    acl_cache: nil,
    active_mode: 100
  ]

  @type multi_state :: :none | :queuing

  @typedoc """
  Cached ACL permissions for the current user. Populated on AUTH and connection
  init, used for O(1) command permission checks without ETS lookups.
  """
  @type acl_cache ::
          %{
            commands: :all | MapSet.t(binary()),
            denied_commands: MapSet.t(binary()),
            keys: :all | [FerricstoreServer.Acl.key_pattern()],
            enabled: boolean()
          }
          | nil

  @type t :: %__MODULE__{
          socket: :inet.socket(),
          transport: module(),
          buffer: binary(),
          client_id: pos_integer(),
          client_name: binary() | nil,
          created_at: integer(),
          peer: {:inet.ip_address(), :inet.port_number()} | nil,
          instance_ctx: FerricStore.Instance.t(),
          stats_counter: reference(),
          multi_state: multi_state(),
          multi_queue: [{binary(), [binary()]}],
          multi_queue_count: non_neg_integer(),
          watched_keys: %{binary() => non_neg_integer()},
          require_auth: boolean(),
          tracking: ClientTracking.tracking_config() | nil,
          acl_cache: acl_cache()
        }

  # ---------------------------------------------------------------------------
  # Ranch protocol entry point
  # ---------------------------------------------------------------------------

  @doc """
  Called by Ranch to start a new connection process.

  ## Parameters

    - `ref`       - Ranch listener ref (used for handshake).
    - `transport` - Transport module (`:ranch_tcp`).
    - `opts`      - Protocol options (unused).
  """
  @spec start_link(ref :: atom(), transport :: module(), opts :: map()) :: {:ok, pid()}
  def start_link(ref, transport, opts) do
    pid = spawn_link(__MODULE__, :init, [ref, transport, opts])
    {:ok, pid}
  end

  @doc false
  @spec init(ref :: atom(), transport :: module(), opts :: map()) :: :ok
  def init(ref, transport, _opts) do
    {:ok, socket} = :ranch.handshake(ref)

    # Enforce require-tls: reject plaintext connections when TLS is required.
    if transport == :ranch_tcp and require_tls?() do
      error_msg =
        Encoder.encode({:error, "ERR TLS required: plaintext connections are not permitted"})

      # Transport accepts iodata directly; no need to flatten to binary.
      ConnSend.send(socket, transport, error_msg, :tls_required)
      transport.close(socket)
    else
      # active: N delivers N TCP messages before the socket goes passive,
      # then sends {:tcp_passive, socket}. We re-arm in the receive loop.
      # N=100 balances throughput (batch of 100 messages without setopts
      # overhead) with back-pressure (mailbox can't grow beyond ~100 messages).
      # active: true has no back-pressure — mailbox can flood under load.
      active_mode = Application.get_env(:ferricstore, :socket_active_mode, 100)
      :ok = transport.setopts(socket, active: active_mode)

      Stats.incr_connections()

      peer =
        case transport.peername(socket) do
          {:ok, addr} -> addr
          _ -> nil
        end

      # Fix 3: Protected mode -- reject non-localhost connections when no ACL
      # users are configured and protected mode is active.
      case FerricstoreServer.Acl.check_protected_mode(peer) do
        {:error, reason} ->
          error_msg = Encoder.encode({:error, reason})
          # Transport accepts iodata directly; no need to flatten to binary.
          ConnSend.send(socket, transport, error_msg, :protected_mode)
          Stats.decr_connections()
          transport.close(socket)

        :ok ->
          if maxclients_exceeded?() do
            error_msg = Encoder.encode({:error, "ERR max number of clients reached"})
            ConnSend.send(socket, transport, error_msg, :maxclients)
            Stats.decr_connections()
            transport.close(socket)
          else
            # Populate ACL cache for the default user at connection init.
            # This avoids ETS lookups on every command for the common case.
            default_cache = ConnAuth.build_acl_cache("default")

            # Join the ACL invalidation process group so we receive
            # {:acl_invalidate, username} messages when ACL rules change.
            join_acl_invalidation_group()

            # Transitional: build instance ctx from global persistent_term state.
            # Will be replaced once listeners pass ctx explicitly.
            ctx = default_instance_ctx()

            state = %__MODULE__{
              socket: socket,
              transport: transport,
              client_id: generate_client_id(),
              client_name: nil,
              created_at: System.monotonic_time(:millisecond),
              peer: peer,
              instance_ctx: ctx,
              stats_counter: ctx.stats_counter,
              require_auth: Ferricstore.Config.get_value("requirepass") != "",
              tracking: ClientTracking.new_config(),
              acl_cache: default_cache,
              active_mode: active_mode
            }

            AuditLog.log(:connection_open, %{
              client_id: state.client_id,
              client_ip: format_peer(peer)
            })

            ConnRegistry.register(state.client_id, self())
            loop(state)
          end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Receive loop (active: N, event-driven)
  # ---------------------------------------------------------------------------

  # Normal receive loop. In active:N mode, the kernel delivers N messages
  # then sends {:tcp_passive, socket}. We re-arm on {:tcp_passive}.
  # In active: :once mode, we re-arm after each data message.
  # In active: true mode, no re-arming needed.
  #
  # Pubsub mode uses a separate loop (pubsub_loop) to avoid checking
  # a mode flag on every iteration of the hot path.
  defp loop(%__MODULE__{socket: socket, transport: transport, active_mode: active_mode} = state) do
    # Re-arm socket for :once mode. For true/N modes, kernel delivers
    # continuously — no re-arm needed (N mode re-arms on {:tcp_passive}).
    if active_mode == :once do
      transport.setopts(socket, active: :once)
    end

    receive do
      {:tcp, ^socket, data} ->
        handle_data(state, data)

      {:ssl, ^socket, data} ->
        handle_data(state, data)

      # Active N mode: socket went passive after N messages, re-arm
      {:tcp_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        loop(state)

      {:ssl_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        loop(state)

      {:tcp_closed, ^socket} ->
        cleanup_connection(state)

      {:tcp_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      {:ssl_closed, ^socket} ->
        cleanup_connection(state)

      {:ssl_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      {:tracking_invalidation, iodata, _keys} ->
        case send_tracked(state, iodata, :tracking_invalidation) do
          :ok -> loop(state)
          {:error, _reason} -> :ok
        end

      :client_kill ->
        cleanup_connection(state)
        transport.close(socket)

      {:acl_invalidate, username} ->
        loop(ConnAuth.maybe_refresh_acl_cache(state, username))
    end
  end

  defp handle_data(%__MODULE__{socket: socket, transport: transport} = state, data) do
    # Avoid binary concatenation when buffer is empty (common case for
    # non-pipelined workloads). Saves one binary allocation + copy per TCP frame.
    buffer = if state.buffer == "", do: data, else: state.buffer <> data

    # Connection buffer limit: reject connections that accumulate too much
    # unparsed data (e.g. sending huge incomplete frames to exhaust memory).
    if byte_size(buffer) > @max_buffer_size do
      send_response(
        socket,
        transport,
        Encoder.encode({:error, "ERR connection buffer overflow (max #{@max_buffer_size} bytes)"})
      )

      cleanup_connection(state)
      transport.close(socket)
    else
      case Parser.parse_commands(buffer) do
        {:ok, [], rest} ->
          loop(%{state | buffer: rest})

        {:ok, commands, rest} ->
          handle_parsed(%{state | buffer: rest}, commands)

        {:error, {:value_too_large, len, max}} ->
          send_response(
            socket,
            transport,
            Encoder.encode({:error, "ERR value too large (#{len} bytes, max #{max} bytes)"})
          )

          cleanup_connection(state)
          transport.close(socket)

        {:error, _reason} ->
          send_response(socket, transport, Encoder.encode({:error, "ERR protocol error"}))
          cleanup_connection(state)
          transport.close(socket)
      end
    end
  end

  defp handle_parsed(%__MODULE__{socket: socket, transport: transport} = state, commands) do
    # Pipeline batch limit: reject batches with too many commands to prevent
    # unbounded memory from accumulated Task results and response buffers.
    max_size = ConnPipeline.max_pipeline_size()

    if length(commands) > max_size do
      send_response(
        socket,
        transport,
        Encoder.encode(
          {:error, "ERR pipeline batch too large (#{length(commands)} commands, max #{max_size})"}
        )
      )

      loop(state)
    else
      case ConnPipeline.pipeline_dispatch(commands, state, &handle_command/2, &send_response/3) do
        {:quit, quit_state} ->
          cleanup_connection(quit_state)
          transport.close(socket)

        {:continue, new_state} ->
          # If SUBSCRIBE was dispatched, switch to the pubsub loop.
          # in_pubsub_mode? is a nil check (O(1)) for non-pubsub connections.
          if in_pubsub_mode?(new_state) do
            pubsub_loop(new_state)
          else
            loop(new_state)
          end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Individual command handlers
  # ---------------------------------------------------------------------------

  @pre_auth_cmds ~w(AUTH HELLO QUIT RESET)

  # Commands that bypass ACL command-level checks. These are protocol-level
  # commands needed for connection setup, teardown, and user switching.
  @acl_bypass_cmds ~w(AUTH HELLO QUIT RESET)

  # Fast path: no auth required, default user with full access — skip ACL checks entirely.
  # This is the common case for 99% of deployments (no requirepass, default user).
  defp handle_command(
         {:command, cmd, args, ast, _keys},
         %{require_auth: false, acl_cache: %{commands: :all, keys: :all}} = state
       )
       when is_binary(cmd) and is_list(args) do
    Stats.incr_commands(state.stats_counter)
    dispatch_parsed(cmd, args, ast, state)
  end

  defp handle_command({:command, cmd, args, ast, keys}, state)
       when is_binary(cmd) and is_list(args) and is_list(keys) do
    handle_command_parts(cmd, args, ast, keys, state)
  end

  defp handle_command(_unknown, state) do
    {:continue, Encoder.encode({:error, "ERR unknown command format"}), state}
  end

  defp handle_command_parts(cmd, args, ast, keys, state) do
    cond do
      requires_auth?(state) and cmd not in @pre_auth_cmds ->
        {:continue, Encoder.encode({:error, "NOAUTH Authentication required."}), state}

      cmd not in @acl_bypass_cmds ->
        with :ok <- ConnAuth.check_command_cached(state.acl_cache, cmd),
             :ok <- ConnAuth.check_keys_cached(state.acl_cache, cmd, keys) do
          Stats.incr_commands(state.stats_counter)
          dispatch_parsed(cmd, args, ast, state)
        else
          {:error, _reason} = err ->
            FerricstoreServer.Acl.log_command_denied(
              state.username,
              cmd,
              format_peer(state.peer),
              state.client_id
            )

            {:continue, Encoder.encode(err), state}
        end

      true ->
        Stats.incr_commands(state.stats_counter)
        dispatch_parsed(cmd, args, ast, state)
    end
  end

  defp requires_auth?(state) do
    not state.authenticated and state.require_auth
  end

  defp dispatch_parsed(cmd, args, ast, state) do
    cond do
      state.multi_state == :queuing and connection_passthrough_ast?(ast) ->
        dispatch_connection_ast(ast, state)

      state.multi_state == :queuing ->
        ConnTransaction.dispatch_queue(cmd, args, ast, state)

      in_pubsub_mode?(state) ->
        dispatch_pubsub_mode_ast(cmd, args, ast, state)

      connection_ast?(ast) ->
        dispatch_connection_ast(ast, state)

      cmd == "GET" and state.transport == :ranch_tcp ->
        dispatch_get_sendfile_ast(args, ast, state)

      blocking_ast?(ast) ->
        dispatch_blocking_ast(ast, args, state)

      cmd == "XREAD" ->
        ConnBlocking.dispatch_xread_ast(ast, args, state)

      ast_store_command?(ast) ->
        dispatch_normal(cmd, args, ast, state)

      true ->
        {:continue,
         Encoder.encode(
           {:error, "ERR unknown command '#{String.downcase(cmd)}', with args beginning with: "}
         ), state}
    end
  end

  # ---------------------------------------------------------------------------
  # Dispatch helpers (called from dispatch table above)
  # ---------------------------------------------------------------------------

  defp dispatch_client_parts(subcmd, rest, state) do
    store =
      if state.sandbox_namespace,
        do: ConnStore.build_store(state.instance_ctx, state.sandbox_namespace),
        else: state.instance_ctx

    conn_state = %{
      client_id: state.client_id,
      client_name: state.client_name,
      created_at: state.created_at,
      peer: state.peer,
      conn_pid: self(),
      tracking: state.tracking
    }

    {result, updated_conn_state} =
      try do
        FerricstoreServer.Commands.Client.handle(subcmd, rest, conn_state, store)
      catch
        :exit, {:noproc, _} ->
          {{:error, "ERR server not ready, shard process unavailable"}, conn_state}

        :exit, {reason, _} ->
          {{:error, "ERR internal error: #{inspect(reason)}"}, conn_state}

        kind, reason ->
          {internal_error(kind, reason), conn_state}
      end

    updated_state = %{
      state
      | client_name: updated_conn_state[:client_name] || state.client_name,
        tracking: updated_conn_state[:tracking] || state.tracking
    }

    {:continue, Encoder.encode(result), updated_state}
  end

  defp dispatch_reset(state) do
    cleanup_pubsub(state)
    ClientTracking.cleanup(self())

    new_state = %{
      state
      | multi_state: :none,
        multi_queue: [],
        multi_queue_count: 0,
        watched_keys: %{},
        sandbox_namespace: nil,
        tracking: ClientTracking.new_config(),
        authenticated: false,
        username: "default",
        pubsub_channels: nil,
        pubsub_patterns: nil,
        acl_cache: ConnAuth.build_acl_cache("default")
    }

    {:continue, Encoder.encode({:simple, "RESET"}), new_state}
  end

  defp dispatch_sandbox([subcmd | rest], state) do
    sandbox_mode = Ferricstore.Config.get_value("sandbox_mode")
    sandbox_enabled? = sandbox_mode in ["local", "enabled"]

    case {subcmd, sandbox_enabled?} do
      {"START", true} ->
        sandbox_start(state)

      {"JOIN", true} ->
        sandbox_join(rest, state)

      {"END", true} ->
        sandbox_end(state)

      {"TOKEN", true} ->
        {:continue, Encoder.encode(state.sandbox_namespace), state}

      {cmd, false} when cmd in ~w(START JOIN END TOKEN) ->
        {:continue,
         Encoder.encode({:error, "ERR SANDBOX commands are not enabled on this server"}), state}

      _ ->
        {:continue, Encoder.encode({:error, "ERR unknown SANDBOX subcommand"}), state}
    end
  end

  defp dispatch_sandbox(_args, state) do
    sandbox_mode = Ferricstore.Config.get_value("sandbox_mode")

    if sandbox_mode in ["local", "enabled"] do
      {:continue, Encoder.encode({:error, "ERR unknown SANDBOX subcommand"}), state}
    else
      {:continue, Encoder.encode({:error, "ERR SANDBOX commands are not enabled on this server"}),
       state}
    end
  end

  defp sandbox_start(state) do
    ns = "test_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    {:continue, Encoder.encode(ns), %{state | sandbox_namespace: ns}}
  end

  defp sandbox_join([token | _], state) do
    {:continue, Encoder.encode(:ok), %{state | sandbox_namespace: token}}
  end

  defp sandbox_join([], state) do
    {:continue, Encoder.encode({:error, "ERR SANDBOX JOIN requires a namespace token"}), state}
  end

  defp sandbox_end(%{sandbox_namespace: nil} = state) do
    {:continue, Encoder.encode({:error, "ERR no active sandbox session"}), state}
  end

  defp sandbox_end(state) do
    ns = state.sandbox_namespace

    try do
      ctx = state.instance_ctx
      keys = Router.keys(ctx)
      Enum.each(keys, fn k -> if String.starts_with?(k, ns), do: Router.delete(ctx, k) end)
    catch
      :exit, _ -> :ok
    end

    {:continue, Encoder.encode(:ok), %{state | sandbox_namespace: nil}}
  end

  defp dispatch_normal(cmd, args, ast, state) do
    # Hot path: pass ctx directly (no closure allocation).
    # Ops and Router handle Instance structs natively.
    # Namespace path: closure map for key prefixing.
    store =
      if state.sandbox_namespace do
        ConnStore.build_store(state.instance_ctx, state.sandbox_namespace)
      else
        state.instance_ctx
      end

    result =
      try do
        dispatch_store_command(cmd, args, ast, store)
      catch
        :exit, {:noproc, _} ->
          {:error, "ERR server not ready, shard process unavailable"}

        :exit, {reason, _} ->
          {:error, "ERR internal error: #{inspect(reason)}"}

        kind, reason ->
          internal_error(kind, reason)
      end

    ConnTracking.maybe_notify_keyspace(cmd, args, result)
    new_state = ConnTracking.maybe_track_read(cmd, args, result, state)
    ConnTracking.maybe_notify_tracking(cmd, args, result, state)

    {:continue, Encoder.encode(result), new_state}
  end

  defp dispatch_store_command(cmd, args, ast, store) do
    if ast_store_command?(ast) do
      Dispatcher.dispatch_ast(ast, store)
    else
      {:error,
       "ERR unsupported command AST for '#{String.downcase(cmd)}' command with #{length(args)} args"}
    end
  end

  defp internal_error(kind, reason),
    do: {:error, "ERR internal error: #{inspect({kind, reason})}"}

  defp dispatch_get_sendfile_ast([key], ast, state)
       when byte_size(key) > 0 and byte_size(key) <= 65_535 do
    ConnSendfile.dispatch_get([key], state, fn _cmd, _args, fallback_state ->
      dispatch_normal("GET", [key], ast, fallback_state)
    end)
  end

  defp dispatch_get_sendfile_ast(args, ast, state), do: dispatch_normal("GET", args, ast, state)

  defp dispatch_pubsub_mode_ast(cmd, args, ast, state) do
    cond do
      pubsub_allowed_connection_ast?(ast) ->
        dispatch_connection_ast(ast, state)

      ast in [:ping] or match?({:ping, _}, ast) ->
        dispatch_normal(cmd, args, ast, state)

      true ->
        {:continue,
         Encoder.encode(
           {:error,
            "ERR Can't execute '#{String.downcase(cmd)}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"}
         ), state}
    end
  end

  defp connection_ast?(ast) when ast in ~w(hello quit reset multi exec discard unwatch)a,
    do: true

  defp connection_ast?({tag, _})
       when tag in ~w(hello auth acl client sandbox subscribe unsubscribe psubscribe punsubscribe watch multi exec discard unwatch reset quit)a,
       do: true

  defp connection_ast?({tag, _, _}) when tag in ~w(auth acl client sandbox)a, do: true
  defp connection_ast?(_ast), do: false

  defp connection_passthrough_ast?(ast) when ast in ~w(multi exec discard unwatch)a, do: true

  defp connection_passthrough_ast?({tag, _}) when tag in ~w(multi exec discard watch unwatch)a,
    do: true

  defp connection_passthrough_ast?({:watch, _keys}), do: true
  defp connection_passthrough_ast?(_ast), do: false

  defp pubsub_allowed_connection_ast?(ast)
       when ast in ~w(quit reset subscribe unsubscribe psubscribe punsubscribe)a,
       do: true

  defp pubsub_allowed_connection_ast?({tag, _})
       when tag in ~w(quit reset subscribe unsubscribe psubscribe punsubscribe)a,
       do: true

  defp pubsub_allowed_connection_ast?(_ast), do: false

  defp dispatch_connection_ast(:hello, state),
    do: {:continue, Encoder.encode(greeting_map(state)), state}

  defp dispatch_connection_ast({:hello, 3}, state),
    do: {:continue, Encoder.encode(greeting_map(state)), state}

  defp dispatch_connection_ast({:hello, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast({:auth, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast({:auth, username, password}, state),
    do: ConnAuth.dispatch_auth([username, password], state)

  defp dispatch_connection_ast({:acl, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast({:acl, subcmd, rest}, state),
    do: ConnAuth.dispatch_acl(subcmd, rest, state)

  defp dispatch_connection_ast({:client, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast({:client, subcmd, rest}, state),
    do: dispatch_client_parts(subcmd, rest, state)

  defp dispatch_connection_ast(:quit, state), do: {:quit, Encoder.encode(:ok), state}

  defp dispatch_connection_ast({:quit, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast(:reset, state), do: dispatch_reset(state)

  defp dispatch_connection_ast({:reset, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast({:sandbox, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast({:sandbox, subcmd, rest}, state),
    do: dispatch_sandbox([subcmd | rest], state)

  defp dispatch_connection_ast({:subscribe, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast({:subscribe, channels}, state),
    do: ConnPubSub.dispatch_subscribe(channels, state)

  defp dispatch_connection_ast({:unsubscribe, channels}, state),
    do: ConnPubSub.dispatch_unsubscribe(channels, state)

  defp dispatch_connection_ast({:psubscribe, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast({:psubscribe, patterns}, state),
    do: ConnPubSub.dispatch_psubscribe(patterns, state)

  defp dispatch_connection_ast({:punsubscribe, patterns}, state),
    do: ConnPubSub.dispatch_punsubscribe(patterns, state)

  defp dispatch_connection_ast(:multi, state), do: ConnTransaction.dispatch_multi([], state)

  defp dispatch_connection_ast({:multi, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast(:exec, state), do: ConnTransaction.dispatch_exec([], state)

  defp dispatch_connection_ast({:exec, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast(:discard, state), do: ConnTransaction.dispatch_discard([], state)

  defp dispatch_connection_ast({:discard, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast({:watch, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp dispatch_connection_ast({:watch, keys}, state),
    do: ConnTransaction.dispatch_watch(keys, state)

  defp dispatch_connection_ast(:unwatch, state), do: ConnTransaction.dispatch_unwatch([], state)

  defp dispatch_connection_ast({:unwatch, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  defp blocking_ast?({tag, _}) when tag in ~w(blpop brpop blmove blmpop)a, do: true
  defp blocking_ast?({tag, _, _}) when tag in ~w(blpop brpop)a, do: true
  defp blocking_ast?({:blmove, _, _, _, _, _}), do: true
  defp blocking_ast?({:blmpop, _, _, _, _}), do: true
  defp blocking_ast?(_ast), do: false

  defp dispatch_blocking_ast({:blpop, error}, _args, state) when elem(error, 0) == :error,
    do: {:continue, Encoder.encode(error), state}

  defp dispatch_blocking_ast({:brpop, error}, _args, state) when elem(error, 0) == :error,
    do: {:continue, Encoder.encode(error), state}

  defp dispatch_blocking_ast({:blmove, error}, _args, state) when elem(error, 0) == :error,
    do: {:continue, Encoder.encode(error), state}

  defp dispatch_blocking_ast({:blmpop, error}, _args, state) when elem(error, 0) == :error,
    do: {:continue, Encoder.encode(error), state}

  defp dispatch_blocking_ast({:blpop, keys, timeout_ms}, _args, state),
    do: ConnBlocking.dispatch_blpop_ast(keys, timeout_ms, state)

  defp dispatch_blocking_ast({:brpop, keys, timeout_ms}, _args, state),
    do: ConnBlocking.dispatch_brpop_ast(keys, timeout_ms, state)

  defp dispatch_blocking_ast(
         {:blmove, source, destination, from_dir, to_dir, timeout_ms},
         _args,
         state
       ),
       do:
         ConnBlocking.dispatch_blmove_ast(
           source,
           destination,
           from_dir,
           to_dir,
           timeout_ms,
           state
         )

  defp dispatch_blocking_ast({:blmpop, keys, direction, count, timeout_ms}, _args, state),
    do: ConnBlocking.dispatch_blmpop_ast(keys, direction, count, timeout_ms, state)

  defp dispatch_blocking_ast(_ast, _args, state),
    do: {:continue, Encoder.encode({:error, "ERR unsupported blocking command AST"}), state}

  defp ast_store_command?({tag, _})
       when tag in ~w(get del exists mget mset incr decr strlen getdel getex msetnx ttl pttl persist lpush rpush lpop llen lpushx rpushx hset hdel hmget hgetall hkeys hvals hlen hrandfield hscan httl hpersist hpttl hexpiretime hgetdel sadd srem smembers smismember scard sinter sunion sdiff sdiffstore sinterstore sunionstore sintercard srandmember spop zrem zcard zpopmin zpopmax zrandmember zmscore bitcount bitop type unlink randomkey expiretime pexpiretime object xadd xlen xread xinfo_stream xinfo xgroup xreadgroup json_set json_get json_del json_numincrby json_type json_strlen json_objkeys json_objlen json_arrappend json_arrlen json_toggle json_clear json_mget geoadd geopos geohash pfadd pfcount pfmerge bf_reserve bf_add bf_madd bf_exists bf_mexists bf_card bf_info cf_reserve cf_add cf_addnx cf_del cf_exists cf_mexists cf_count cf_info cms_initbydim cms_initbyprob cms_incrby cms_query cms_merge cms_info topk_reserve topk_add topk_incrby topk_query topk_list topk_count topk_info tdigest_create tdigest_add tdigest_reset tdigest_quantile tdigest_cdf tdigest_rank tdigest_revrank tdigest_byrank tdigest_byrevrank tdigest_trimmed_mean tdigest_min tdigest_max tdigest_info tdigest_merge ping echo dbsize keys flushdb flushall info command select lolwut debug slowlog save bgsave lastsave config module waitaof cas lock unlock extend ratelimit_add ferricstore_key_info fetch_or_compute fetch_or_compute_result fetch_or_compute_error cluster_health cluster_stats cluster_keyslot cluster_slots cluster_status cluster_join cluster_leave cluster_failover cluster_promote cluster_demote cluster_role ferricstore_hotness ferricstore_config ferricstore_metrics memory publish pubsub)a,
       do: true

  defp ast_store_command?({tag, _, _})
       when tag in ~w(set incrby decrby incrbyfloat append getset getex setnx expire pexpire expireat pexpireat lpop rpop lindex rpoplpush hget hexists hstrlen hrandfield hexpire hpexpire httl hpersist hpttl hexpiretime hgetdel hgetex sismember srandmember spop sscan sintercard zscore zrank zrevrank zadd zcount zpopmin zpopmax zscan zrangebyscore zrevrangebyscore getbit bitcount bitpos rename renamenx scan object wait xadd xrange xrevrange xtrim xdel xgroup json_get json_del json_numincrby json_type json_strlen json_objkeys json_objlen json_arrlen json_toggle json_clear json_mget geosearch bf_reserve cf_reserve cms_initbydim cms_initbyprob cms_incrby cms_merge topk_reserve topk_incrby topk_list tdigest_create tdigest_add tdigest_quantile tdigest_cdf tdigest_rank tdigest_revrank tdigest_byrank tdigest_byrevrank tdigest_trimmed_mean tdigest_merge cas lock unlock extend fetch_or_compute fetch_or_compute_result fetch_or_compute_error ferricstore_key_info ratelimit_add)a,
       do: true

  defp ast_store_command?({tag, _, _, _})
       when tag in ~w(set setex psetex getrange setrange expire pexpire expireat pexpireat lrange lset lrem ltrim hincrby hincrbyfloat hsetnx hrandfield hexpire hpexpire hgetex hsetex smove sscan zadd zincrby zcount zrange zrevrange zrandmember zscan zrangebyscore zrevrangebyscore setbit bitpos bitop copy object xadd xread xreadgroup xack json_numincrby json_arrappend geoadd geosearchstore bf_reserve cms_initbydim cms_initbyprob cms_merge tdigest_trimmed_mean tdigest_merge lock unlock extend fetch_or_compute fetch_or_compute_result fetch_or_compute_error)a,
       do: true

  defp ast_store_command?({tag, _, _, _, _})
       when tag in ~w(linsert lmove zrange zrevrange zrangebyscore zrevrangebyscore xrange xrevrange xgroup_create json_set geodist cas ratelimit_add)a,
       do: true

  defp ast_store_command?({tag, _, _, _, _, _})
       when tag in ~w(topk_reserve)a,
       do: true

  defp ast_store_command?(tag) when tag in ~w(ping)a, do: true
  defp ast_store_command?(_ast), do: false

  # ---------------------------------------------------------------------------
  # Greeting map
  # ---------------------------------------------------------------------------

  defp greeting_map(state) do
    %{
      "server" => "ferricstore",
      "version" => "0.1.0",
      "proto" => 3,
      "id" => state.client_id,
      "mode" => "standalone",
      "role" => "master",
      "modules" => []
    }
  end

  defp generate_client_id do
    :erlang.unique_integer([:positive])
  end

  # ---------------------------------------------------------------------------
  # Response sending
  # ---------------------------------------------------------------------------

  defp send_response(socket, transport, iodata) do
    _ = ConnSend.send(socket, transport, iodata, :response)
    :ok
  end

  defp send_tracked(%__MODULE__{socket: socket, transport: transport} = state, iodata, phase) do
    metadata = %{client_id: state.client_id}

    case ConnSend.send(socket, transport, iodata, phase, metadata) do
      :ok ->
        :ok

      {:error, _reason} = error ->
        cleanup_connection(state)
        transport.close(socket)
        error
    end
  end

  # ---------------------------------------------------------------------------
  # Pub/Sub mode loop
  # ---------------------------------------------------------------------------

  defp pubsub_loop(
         %__MODULE__{socket: socket, transport: transport, active_mode: active_mode} = state
       ) do
    # No setopts needed — active mode (true/N/:once) is maintained from
    # the main loop. TCP data keeps arriving and is handled below.
    if active_mode == :once do
      transport.setopts(socket, active: :once)
    end

    receive do
      {:tcp, ^socket, data} ->
        handle_data(state, data)

      {:ssl, ^socket, data} ->
        handle_data(state, data)

      {:tcp_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        pubsub_loop(state)

      {:ssl_passive, ^socket} ->
        transport.setopts(socket, active: active_mode)
        pubsub_loop(state)

      {:tcp_closed, ^socket} ->
        cleanup_connection(state)

      {:tcp_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      {:ssl_closed, ^socket} ->
        cleanup_connection(state)

      {:ssl_error, ^socket, _reason} ->
        cleanup_connection(state)
        transport.close(socket)

      {:pubsub_message, channel, message} ->
        push = {:push, ["message", channel, message]}

        case send_tracked(state, Encoder.encode(push), :pubsub_message) do
          :ok -> pubsub_loop(state)
          {:error, _reason} -> :ok
        end

      {:pubsub_pmessage, pattern, channel, message} ->
        push = {:push, ["pmessage", pattern, channel, message]}

        case send_tracked(state, Encoder.encode(push), :pubsub_pmessage) do
          :ok -> pubsub_loop(state)
          {:error, _reason} -> :ok
        end

      {:tracking_invalidation, iodata, _keys} ->
        case send_tracked(state, iodata, :tracking_invalidation) do
          :ok -> pubsub_loop(state)
          {:error, _reason} -> :ok
        end

      :client_kill ->
        cleanup_connection(state)
        transport.close(socket)

      {:acl_invalidate, username} ->
        pubsub_loop(ConnAuth.maybe_refresh_acl_cache(state, username))
    end
  end

  defp in_pubsub_mode?(%{pubsub_channels: nil}), do: false

  defp in_pubsub_mode?(state),
    do: MapSet.size(state.pubsub_channels) > 0 or MapSet.size(state.pubsub_patterns) > 0

  defp cleanup_connection(state) do
    duration_ms = System.monotonic_time(:millisecond) - state.created_at

    AuditLog.log(:connection_close, %{
      client_id: state.client_id,
      client_ip: format_peer(state.peer),
      duration_ms: duration_ms
    })

    cleanup_pubsub(state)
    ClientTracking.cleanup(self())
    Ferricstore.Commands.Stream.cleanup_stream_waiters(self())
    ConnRegistry.unregister(state.client_id, self())
    Stats.decr_connections()
  end

  defp cleanup_pubsub(state) do
    if state.pubsub_channels, do: Enum.each(state.pubsub_channels, &PS.unsubscribe(&1, self()))
    if state.pubsub_patterns, do: Enum.each(state.pubsub_patterns, &PS.punsubscribe(&1, self()))
  end

  # ---------------------------------------------------------------------------
  # Instance context helpers
  # ---------------------------------------------------------------------------

  # Transitional: build instance ctx from persistent_term global state.
  # Will be removed once listeners pass ctx explicitly at connection init.
  @spec default_instance_ctx() :: FerricStore.Instance.t()
  defp default_instance_ctx do
    FerricStore.Instance.get(:default)
  end

  # Formats a peer tuple `{ip, port}` into a human-readable string.
  defp format_peer(nil), do: "unknown"
  defp format_peer({ip, port}), do: "#{:inet.ntoa(ip)}:#{port}"

  # Returns true when the require-tls configuration flag is set.
  defp require_tls? do
    Application.get_env(:ferricstore, :require_tls, false) == true
  end

  defp maxclients_exceeded? do
    maxclients = Application.get_env(:ferricstore, :maxclients, 10_000)
    is_integer(maxclients) and maxclients > 0 and Stats.active_connections() > maxclients
  end

  # ---------------------------------------------------------------------------
  # ACL cache — delegated to Connection.Auth
  # ---------------------------------------------------------------------------

  # The process group name for ACL invalidation broadcasts.
  @acl_pg_group :ferricstore_acl_connections

  @doc false
  @spec acl_pg_group() :: atom()
  def acl_pg_group, do: @acl_pg_group

  # Joins the OTP :pg process group for ACL invalidation broadcasts.
  # Called once during connection init. The process is automatically removed
  # from the group when it terminates (no explicit leave needed).
  # The :pg scope is started by FerricstoreServer.Application.
  @spec join_acl_invalidation_group() :: :ok
  defp join_acl_invalidation_group do
    :pg.join(@acl_pg_group, @acl_pg_group, self())
    :ok
  end
end
