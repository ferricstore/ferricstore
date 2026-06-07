defmodule FerricstoreServer.Connection do
  @moduledoc """
  Ranch protocol handler for a single FerricStore client connection.

  Each accepted TCP connection spawns one `Connection` process. The process:

  1. Performs the `CLIENT HELLO 3` handshake (RESP3-only; rejects RESP2).
  2. Enters a receive loop, accumulating TCP chunks into a binary buffer.
  3. Parses all complete RESP3 frames from the buffer via `FerricstoreServer.Resp.Parser`.
  4. Dispatches commands using a segmented pipeline:
     - Hot pure batches such as GET, SET, mixed GET/SET, Flow, Streams, and
       selected write groups use batch dispatch paths.
     - Generic pure fallback stays in the connection process and coalesces
       encoded replies into one socket send unless a large streaming response
       is required. Avoiding per-command worker tasks keeps scheduler pressure
       predictable under deep pipelines.
     - Stateful commands (MULTI, AUTH, SUBSCRIBE, blocking ops, etc.) act
       as barriers: prior pure commands are flushed before the stateful command
       executes synchronously.
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
  alias FerricstoreServer.Connection.Commands, as: ConnCommands
  alias FerricstoreServer.Connection.Dashboard, as: ConnDashboard
  alias FerricstoreServer.Connection.Pipeline, as: ConnPipeline
  alias FerricstoreServer.Connection.PubSubSession, as: ConnPubSubSession
  alias FerricstoreServer.Connection.Registry, as: ConnRegistry
  alias FerricstoreServer.Connection.Send, as: ConnSend
  alias FerricstoreServer.Connection.Sendfile, as: ConnSendfile
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Connection.Tracking, as: ConnTracking
  alias FerricstoreServer.Connection.Transaction, as: ConnTransaction

  alias Ferricstore.PubSub, as: PS

  require Logger

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
    multi_error: false,
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
          multi_queue: [{binary(), [binary()], term(), [binary()]}],
          multi_queue_count: non_neg_integer(),
          multi_error: boolean(),
          watched_keys: %{binary() => term()},
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
              require_auth: ConnAuth.user_requires_auth?("default"),
              tracking: ClientTracking.new_config(),
              acl_cache: default_cache,
              active_mode: active_mode
            }

            AuditLog.log(:connection_open, %{
              client_id: state.client_id,
              client_ip: format_peer(peer)
            })

            ConnRegistry.register(state.client_id, self(), ConnDashboard.summary(state))
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
  def loop(%__MODULE__{socket: socket, transport: transport, active_mode: active_mode} = state) do
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
        refreshed_state =
          state
          |> ConnAuth.maybe_refresh_acl_cache(username)
          |> enforce_pubsub_acl_after_refresh()

        loop(maybe_sync_connection_registry(state, refreshed_state))
    end
  end

  def handle_data(%__MODULE__{socket: socket, transport: transport} = state, data) do
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
          new_state = maybe_sync_connection_registry(state, new_state)

          # If SUBSCRIBE was dispatched, switch to the pubsub loop.
          # in_pubsub_mode? is a nil check (O(1)) for non-pubsub connections.
          cond do
            in_pubsub_mode?(new_state) ->
              pubsub_loop(new_state)

            new_state.buffer != "" ->
              buffered = new_state.buffer
              handle_data(%{new_state | buffer: ""}, buffered)

            true ->
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

  # Fast path: no auth required, default user with canonical full access.
  defp handle_command(
         {:command, cmd, args, ast, _keys},
         %{require_auth: false, acl_cache: :full_access} = state
       )
       when is_binary(cmd) and is_list(args) do
    if live_requirepass_enabled?() do
      handle_command_parts(cmd, args, ast, [], state)
    else
      Stats.incr_commands(state.stats_counter)
      dispatch_parsed(cmd, args, ast, [], state)
    end
  end

  defp handle_command({:command, cmd, args, ast, keys}, state)
       when is_binary(cmd) and is_list(args) and is_list(keys) do
    handle_command_parts(cmd, args, ast, keys, state)
  end

  defp handle_command(_unknown, state) do
    {:continue, Encoder.encode({:error, "ERR unknown command format"}), state}
  end

  defp handle_command_parts(cmd, args, ast, keys, state) do
    acl_cmd = ConnAuth.acl_command_name(cmd, args, ast)

    cond do
      requires_auth?(state) and acl_cmd not in @pre_auth_cmds ->
        {:continue, Encoder.encode({:error, "NOAUTH Authentication required."}), state}

      acl_cmd not in @acl_bypass_cmds ->
        with :ok <- ConnAuth.check_command_cached(state.acl_cache, acl_cmd),
             :ok <- ConnAuth.check_keys_cached(state.acl_cache, acl_cmd, acl_key_args(cmd, keys)),
             :ok <- ConnAuth.check_channels_cached(state.acl_cache, acl_channel_args(cmd, args)) do
          Stats.incr_commands(state.stats_counter)
          dispatch_parsed(cmd, args, ast, keys, state)
        else
          {:error, _reason} = err ->
            FerricstoreServer.Acl.log_command_denied(
              state.username,
              acl_cmd,
              format_peer(state.peer),
              state.client_id
            )

            state = maybe_mark_multi_queue_error(state, ast)
            {:continue, Encoder.encode(err), state}
        end

      true ->
        Stats.incr_commands(state.stats_counter)
        dispatch_parsed(cmd, args, ast, keys, state)
    end
  end

  defp requires_auth?(state) do
    not state.authenticated and (state.require_auth or live_requirepass_enabled?())
  end

  defp maybe_mark_multi_queue_error(%{multi_state: :queuing} = state, ast) do
    if connection_passthrough_ast?(ast) do
      state
    else
      %{state | multi_error: true}
    end
  end

  defp maybe_mark_multi_queue_error(state, _ast), do: state

  defp live_requirepass_enabled? do
    Ferricstore.Config.get_value("requirepass") not in [nil, ""]
  end

  defp dispatch_parsed(cmd, args, ast, keys, state) do
    cond do
      state.multi_state == :queuing and connection_passthrough_ast?(ast) ->
        dispatch_connection_ast(ast, state)

      state.multi_state == :queuing ->
        ConnTransaction.dispatch_queue(cmd, args, ast, keys, state)

      in_pubsub_mode?(state) ->
        dispatch_pubsub_mode_ast(cmd, args, ast, state)

      connection_ast?(ast) ->
        dispatch_connection_command(cmd, args, ast, state)

      cmd == "GET" and state.transport in [:ranch_tcp, :ranch_ssl] ->
        dispatch_get_sendfile_ast(args, ast, state)

      cmd == "MGET" and state.transport in [:ranch_tcp, :ranch_ssl] ->
        dispatch_mget_sendfile_ast(args, ast, state)

      cmd == "GETRANGE" and state.transport in [:ranch_tcp, :ranch_ssl] ->
        dispatch_getrange_sendfile_ast(args, ast, state)

      blocking_ast?(ast) ->
        dispatch_blocking_ast(ast, args, state)

      cmd in ["XREAD", "XREADGROUP"] ->
        ConnBlocking.dispatch_stream_read_ast(cmd, ast, args, state)

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
        dispatch_store_command(cmd, args, ast, store, state.instance_ctx, state.sandbox_namespace)
      catch
        :exit, {:noproc, _} ->
          {:error, "ERR server not ready, shard process unavailable"}

        :exit, {reason, _} ->
          internal_error(:exit, reason)

        kind, reason ->
          internal_error(kind, reason)
      end

    ConnTracking.maybe_notify_keyspace(cmd, args, result)
    new_state = ConnTracking.maybe_track_read(cmd, args, result, state)
    ConnTracking.maybe_notify_tracking(cmd, args, result, state)

    {:continue, Encoder.encode(result), new_state}
  end

  defp dispatch_store_command(
         _cmd,
         _args,
         {:pfadd, [key | elements]},
         _store,
         ctx,
         namespace
       ) do
    Router.pfadd(ctx, namespace_key(namespace, key), elements)
  end

  defp dispatch_store_command(
         _cmd,
         _args,
         {:pfmerge, [dest_key | source_keys]},
         _store,
         ctx,
         namespace
       ) do
    Router.pfmerge(
      ctx,
      namespace_key(namespace, dest_key),
      namespace_keys(namespace, source_keys)
    )
  end

  defp dispatch_store_command(
         _cmd,
         _args,
         {:json_set, key, path, value, flags},
         _store,
         ctx,
         namespace
       ) do
    Router.json_set(ctx, namespace_key(namespace, key), path, value, flags)
  end

  defp dispatch_store_command(
         _cmd,
         _args,
         {:json_del, key, path},
         _store,
         ctx,
         namespace
       ) do
    Router.json_del(ctx, namespace_key(namespace, key), path)
  end

  defp dispatch_store_command(
         _cmd,
         _args,
         {:json_numincrby, key, path, increment},
         _store,
         ctx,
         namespace
       ) do
    Router.json_numincrby(ctx, namespace_key(namespace, key), path, increment)
  end

  defp dispatch_store_command(
         _cmd,
         _args,
         {:json_arrappend, key, path, values},
         _store,
         ctx,
         namespace
       ) do
    Router.json_arrappend(ctx, namespace_key(namespace, key), path, values)
  end

  defp dispatch_store_command(
         _cmd,
         _args,
         {:json_toggle, key, path},
         _store,
         ctx,
         namespace
       ) do
    Router.json_toggle(ctx, namespace_key(namespace, key), path)
  end

  defp dispatch_store_command(
         _cmd,
         _args,
         {:json_clear, key, path},
         _store,
         ctx,
         namespace
       ) do
    Router.json_clear(ctx, namespace_key(namespace, key), path)
  end

  defp dispatch_store_command(cmd, args, ast, store, _ctx, _namespace) do
    if ast_store_command?(ast) do
      Dispatcher.dispatch_ast(ast, store)
    else
      {:error,
       "ERR unsupported command AST for '#{String.downcase(cmd)}' command with #{length(args)} args"}
    end
  end

  defp namespace_key(nil, key), do: key
  defp namespace_key(namespace, key) when is_binary(namespace), do: namespace <> key

  defp namespace_keys(nil, keys), do: keys

  defp namespace_keys(namespace, keys) when is_binary(namespace),
    do: Enum.map(keys, &(namespace <> &1))

  def internal_error(kind, reason) do
    Logger.error(fn ->
      "FerricStore connection internal error: #{inspect({kind, reason}, limit: 20)}"
    end)

    {:error, "ERR internal error"}
  end

  defp dispatch_get_sendfile_ast([key], ast, state)
       when byte_size(key) > 0 and byte_size(key) <= 65_535 do
    ConnSendfile.dispatch_get([key], state, fn _cmd, _args, fallback_state ->
      dispatch_normal("GET", [key], ast, fallback_state)
    end)
  end

  defp dispatch_get_sendfile_ast(args, ast, state), do: dispatch_normal("GET", args, ast, state)

  defp dispatch_mget_sendfile_ast(args, ast, state) do
    ConnSendfile.dispatch_mget(args, state, fn _cmd, _args, fallback_state ->
      dispatch_normal("MGET", args, ast, fallback_state)
    end)
  end

  defp dispatch_getrange_sendfile_ast(
         [key, _start_arg, _end_arg] = args,
         {:getrange, key, start_idx, end_idx} = ast,
         state
       )
       when is_binary(key) and is_integer(start_idx) and is_integer(end_idx) do
    ConnSendfile.dispatch_getrange(args, key, start_idx, end_idx, state, fn fallback_state ->
      dispatch_normal("GETRANGE", args, ast, fallback_state)
    end)
  end

  defp dispatch_getrange_sendfile_ast(args, _ast, state) when length(args) != 3 do
    {:continue, Encoder.encode({:error, "ERR wrong number of arguments for 'getrange' command"}),
     state}
  end

  defp dispatch_getrange_sendfile_ast(args, ast, state),
    do: dispatch_normal("GETRANGE", args, ast, state)

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

  defp connection_passthrough_ast?(ast) when ast in ~w(quit reset multi exec discard unwatch)a,
    do: true

  defp connection_passthrough_ast?({tag, _})
       when tag in ~w(quit reset multi exec discard watch unwatch)a,
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

  defp dispatch_connection_command(cmd, args, ast, state),
    do: ConnCommands.dispatch_connection_command(cmd, args, ast, state)

  defp dispatch_connection_ast(ast, state), do: ConnCommands.dispatch_connection_ast(ast, state)

  defp blocking_ast?({tag, _}) when tag in ~w(blpop brpop blmove blmpop)a, do: true
  defp blocking_ast?({tag, _, _}) when tag in ~w(blpop brpop)a, do: true
  defp blocking_ast?({:blmove, _, _, _, _, _}), do: true
  defp blocking_ast?({:blmpop, _, _, _, _}), do: true

  defp blocking_ast?({:flow_claim_due, _type, opts}) when is_list(opts),
    do: Keyword.has_key?(opts, :block_ms)

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

  defp dispatch_blocking_ast({:flow_claim_due, type, opts}, _args, state),
    do: ConnBlocking.dispatch_flow_claim_due_ast(type, opts, state)

  defp dispatch_blocking_ast(_ast, _args, state),
    do: {:continue, Encoder.encode({:error, "ERR unsupported blocking command AST"}), state}

  defp ast_store_command?({tag, _})
       when tag in ~w(get del exists mget mset incr decr strlen getdel getex msetnx ttl pttl persist lpush rpush lpop rpop llen lpos lpushx rpushx hset hdel hmget hgetall hkeys hvals hlen hrandfield hscan httl hpersist hpttl hexpiretime hgetdel sadd srem smembers smismember scard sinter sunion sdiff sdiffstore sinterstore sunionstore sintercard srandmember spop zrem zcard zpopmin zpopmax zrandmember zmscore bitcount bitop type unlink randomkey expiretime pexpiretime object xadd xlen xread xinfo_stream xinfo xgroup xreadgroup json_set json_get json_del json_numincrby json_type json_strlen json_objkeys json_objlen json_arrappend json_arrlen json_toggle json_clear json_mget geoadd geopos geohash pfadd pfcount pfmerge bf_reserve bf_add bf_madd bf_exists bf_mexists bf_card bf_info cf_reserve cf_add cf_addnx cf_del cf_exists cf_mexists cf_count cf_info cms_initbydim cms_initbyprob cms_incrby cms_query cms_merge cms_info topk_reserve topk_add topk_incrby topk_query topk_list topk_count topk_info tdigest_create tdigest_add tdigest_reset tdigest_quantile tdigest_cdf tdigest_rank tdigest_revrank tdigest_byrank tdigest_byrevrank tdigest_trimmed_mean tdigest_min tdigest_max tdigest_info tdigest_merge ping echo dbsize keys flushdb flushall info command select lolwut debug slowlog save bgsave lastsave config module waitaof cas lock unlock extend ratelimit_add ferricstore_key_info fetch_or_compute fetch_or_compute_result fetch_or_compute_error cluster_health cluster_stats cluster_keyslot cluster_slots cluster_status cluster_join cluster_leave cluster_failover cluster_promote cluster_demote cluster_role ferricstore_hotness ferricstore_config ferricstore_metrics memory publish pubsub flow_create flow_get flow_claim_due flow_reclaim flow_complete flow_transition flow_retry flow_fail flow_cancel flow_rewind flow_list flow_info flow_stuck flow_history)a,
       do: true

  defp ast_store_command?({tag, _, _})
       when tag in ~w(set incrby decrby incrbyfloat append getset getex setnx expire pexpire expireat pexpireat lpop rpop lindex lpos rpoplpush hget hexists hstrlen hrandfield hexpire hpexpire httl hpersist hpttl hexpiretime hgetdel hgetex sismember srandmember spop sscan sintercard zscore zrank zrevrank zadd zcount zpopmin zpopmax zscan zrangebyscore zrevrangebyscore getbit bitcount bitpos rename renamenx scan object wait xadd xrange xrevrange xtrim xdel xgroup json_get json_del json_numincrby json_type json_strlen json_objkeys json_objlen json_arrlen json_toggle json_clear json_mget geosearch bf_reserve cf_reserve cms_initbydim cms_initbyprob cms_incrby cms_merge topk_reserve topk_incrby topk_list tdigest_create tdigest_add tdigest_quantile tdigest_cdf tdigest_rank tdigest_revrank tdigest_byrank tdigest_byrevrank tdigest_trimmed_mean tdigest_merge cas lock unlock extend fetch_or_compute fetch_or_compute_result fetch_or_compute_error ferricstore_key_info ratelimit_add flow_create flow_value_put flow_signal flow_get flow_policy_set flow_policy_get flow_claim_due flow_reclaim flow_cancel flow_rewind flow_list flow_terminals flow_failures flow_by_parent flow_by_root flow_by_correlation flow_info flow_stuck flow_history)a,
       do: true

  defp ast_store_command?({tag, _, _, _})
       when tag in ~w(set setex psetex getrange setrange expire pexpire expireat pexpireat lrange lset lrem ltrim hincrby hincrbyfloat hsetnx hrandfield hscan hexpire hpexpire hgetex hsetex smove sscan zadd zincrby zcount zrange zrevrange zrandmember zscan zrangebyscore zrevrangebyscore setbit bitpos bitop copy object xadd xread xreadgroup xack json_numincrby json_arrappend geoadd geosearchstore bf_reserve cms_initbydim cms_initbyprob cms_merge tdigest_trimmed_mean tdigest_merge lock unlock extend fetch_or_compute fetch_or_compute_result fetch_or_compute_error flow_create_many flow_spawn_children flow_extend_lease flow_complete flow_complete_many flow_retry flow_retry_many flow_fail flow_fail_many flow_cancel_many)a,
       do: true

  defp ast_store_command?({tag, _, _, _, _})
       when tag in ~w(linsert lmove zrange zrevrange zrangebyscore zrevrangebyscore xrange xrevrange xgroup_create json_set geodist cas ratelimit_add flow_transition flow_transition_many)a,
       do: true

  defp ast_store_command?({tag, _, _, _, _, _})
       when tag in ~w(topk_reserve flow_transition_many)a,
       do: true

  # Rust emits `{known_tag, raw_args}` for known commands whose arity does not
  # match a typed AST. Let the core dispatcher produce the Redis arity error.
  defp ast_store_command?({tag, args}) when is_atom(tag) and is_list(args), do: true

  defp ast_store_command?(tag) when tag in ~w(ping)a, do: true
  defp ast_store_command?(_ast), do: false

  # ---------------------------------------------------------------------------
  # Greeting map
  # ---------------------------------------------------------------------------

  def greeting_map(state) do
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
    ConnSend.send(socket, transport, iodata, :response)
  end

  def send_tracked(%__MODULE__{socket: socket, transport: transport} = state, iodata, phase) do
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

  defp pubsub_loop(state), do: ConnPubSubSession.pubsub_loop(state)

  defp in_pubsub_mode?(state), do: ConnPubSubSession.in_pubsub_mode?(state)

  defp enforce_pubsub_acl_after_refresh(state),
    do: ConnPubSubSession.enforce_pubsub_acl_after_refresh(state)

  defp acl_key_args(cmd, _keys)
       when cmd in ~w(PUBLISH SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE),
       do: []

  defp acl_key_args(_cmd, keys), do: keys

  defp acl_channel_args("PUBLISH", [channel | _]) when is_binary(channel), do: [channel]
  defp acl_channel_args(cmd, args) when cmd in ~w(SUBSCRIBE PSUBSCRIBE), do: args
  defp acl_channel_args(_cmd, _args), do: []

  def cleanup_connection(state) do
    duration_ms = System.monotonic_time(:millisecond) - state.created_at

    AuditLog.log(:connection_close, %{
      client_id: state.client_id,
      client_ip: format_peer(state.peer),
      duration_ms: duration_ms
    })

    cleanup_pubsub(state)
    ClientTracking.cleanup(self())
    Ferricstore.Commands.Stream.cleanup_stream_waiters(self())
    Ferricstore.Flow.ClaimWaiters.cleanup(self())
    ConnRegistry.unregister(state.client_id, self())
    Stats.decr_connections()
  end

  def cleanup_pubsub(state) do
    if state.pubsub_channels != nil or state.pubsub_patterns != nil do
      PS.cleanup(self())
    end
  end

  def maybe_sync_connection_registry(old_state, new_state) do
    if ConnDashboard.summary(old_state) != ConnDashboard.summary(new_state) do
      ConnRegistry.update(new_state.client_id, self(), ConnDashboard.summary(new_state))
    end

    new_state
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
