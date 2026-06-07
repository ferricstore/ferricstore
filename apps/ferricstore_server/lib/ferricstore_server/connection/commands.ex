defmodule FerricstoreServer.Connection.Commands do
  @moduledoc false

  alias Ferricstore.Store.Router
  alias FerricstoreServer.ClientTracking
  alias FerricstoreServer.Connection
  alias FerricstoreServer.Connection.Auth, as: ConnAuth
  alias FerricstoreServer.Connection.PubSub, as: ConnPubSub
  alias FerricstoreServer.Connection.Store, as: ConnStore
  alias FerricstoreServer.Connection.Transaction, as: ConnTransaction
  alias FerricstoreServer.Resp.Encoder

  def dispatch_client_parts(subcmd, rest, state) do
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
          {Connection.internal_error(:exit, reason), conn_state}

        kind, reason ->
          {Connection.internal_error(kind, reason), conn_state}
      end

    updated_state = %{
      state
      | client_name: updated_conn_state[:client_name] || state.client_name,
        tracking: updated_conn_state[:tracking] || state.tracking
    }

    {:continue, Encoder.encode(result), updated_state}
  end

  def dispatch_reset(state) do
    Connection.cleanup_pubsub(state)
    ClientTracking.cleanup(self())

    new_state = %{
      state
      | multi_state: :none,
        multi_queue: [],
        multi_queue_count: 0,
        multi_error: false,
        watched_keys: %{},
        sandbox_namespace: nil,
        tracking: ClientTracking.new_config(),
        authenticated: false,
        username: "default",
        pubsub_channels: nil,
        pubsub_patterns: nil,
        require_auth: ConnAuth.user_requires_auth?("default"),
        acl_cache: ConnAuth.build_acl_cache("default")
    }

    {:continue, Encoder.encode({:simple, "RESET"}), new_state}
  end

  def dispatch_sandbox([subcmd | rest], state) do
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

  def dispatch_sandbox(_args, state) do
    sandbox_mode = Ferricstore.Config.get_value("sandbox_mode")

    if sandbox_mode in ["local", "enabled"] do
      {:continue, Encoder.encode({:error, "ERR unknown SANDBOX subcommand"}), state}
    else
      {:continue, Encoder.encode({:error, "ERR SANDBOX commands are not enabled on this server"}),
       state}
    end
  end

  def sandbox_start(state) do
    ns = "test_" <> Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    {:continue, Encoder.encode(ns), %{state | sandbox_namespace: ns}}
  end

  def sandbox_join([token | _], state) do
    {:continue, Encoder.encode(:ok), %{state | sandbox_namespace: token}}
  end

  def sandbox_join([], state) do
    {:continue, Encoder.encode({:error, "ERR SANDBOX JOIN requires a namespace token"}), state}
  end

  def sandbox_end(%{sandbox_namespace: nil} = state) do
    {:continue, Encoder.encode({:error, "ERR no active sandbox session"}), state}
  end

  def sandbox_end(state) do
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

  def dispatch_connection_command("HELLO", args, ast, state),
    do: dispatch_hello(args, ast, state)

  def dispatch_connection_command("CLIENT", ["HELLO" | args], ast, state),
    do: dispatch_hello(args, ast, state)

  def dispatch_connection_command(_cmd, _args, ast, state),
    do: dispatch_connection_ast(ast, state)

  def dispatch_hello(_args, {:hello, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_hello(args, ast, state) when ast in [:hello, {:hello, 3}] do
    case hello_auth_args(args) do
      {:ok, nil} ->
        {:continue, Encoder.encode(Connection.greeting_map(state)), state}

      {:ok, {username, password}} ->
        case ConnAuth.dispatch_auth([username, password], state) do
          {:continue, _auth_reply, %{authenticated: true} = auth_state} ->
            {:continue, Encoder.encode(Connection.greeting_map(auth_state)), auth_state}

          other ->
            other
        end

      {:error, reason} ->
        {:continue, Encoder.encode({:error, reason}), state}
    end
  end

  def dispatch_hello(_args, ast, state), do: dispatch_connection_ast(ast, state)

  def hello_auth_args([]), do: {:ok, nil}
  def hello_auth_args(["3"]), do: {:ok, nil}

  def hello_auth_args(["3" | rest]), do: hello_auth_args_after_version(rest, nil)

  def hello_auth_args(_args),
    do: {:error, "NOPROTO this server does not support the requested protocol version"}

  def hello_auth_args_after_version([], auth), do: {:ok, auth}

  def hello_auth_args_after_version([option, username, password | rest], _auth)
       when is_binary(option) and is_binary(username) and is_binary(password) do
    case String.upcase(option) do
      "AUTH" -> hello_auth_args_after_version(rest, {username, password})
      _other -> {:error, "ERR Syntax error in HELLO option '#{option}'"}
    end
  end

  def hello_auth_args_after_version([option | _rest], _auth) when is_binary(option),
    do: {:error, "ERR Syntax error in HELLO option '#{option}'"}

  def dispatch_connection_ast(:hello, state),
    do: {:continue, Encoder.encode(Connection.greeting_map(state)), state}

  def dispatch_connection_ast({:hello, 3}, state),
    do: {:continue, Encoder.encode(Connection.greeting_map(state)), state}

  def dispatch_connection_ast({:hello, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast({:auth, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast({:auth, username, password}, state),
    do: ConnAuth.dispatch_auth([username, password], state)

  def dispatch_connection_ast({:acl, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast({:acl, subcmd, rest}, state),
    do: ConnAuth.dispatch_acl(subcmd, rest, state)

  def dispatch_connection_ast({:client, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast({:client, subcmd, rest}, state),
    do: dispatch_client_parts(subcmd, rest, state)

  def dispatch_connection_ast(:quit, state), do: {:quit, Encoder.encode(:ok), state}

  def dispatch_connection_ast({:quit, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast(:reset, state), do: dispatch_reset(state)

  def dispatch_connection_ast({:reset, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast({:sandbox, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast({:sandbox, subcmd, rest}, state),
    do: dispatch_sandbox([subcmd | rest], state)

  def dispatch_connection_ast({:subscribe, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast({:subscribe, channels}, state),
    do: ConnPubSub.dispatch_subscribe(channels, state)

  def dispatch_connection_ast({:unsubscribe, channels}, state),
    do: ConnPubSub.dispatch_unsubscribe(channels, state)

  def dispatch_connection_ast({:psubscribe, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast({:psubscribe, patterns}, state),
    do: ConnPubSub.dispatch_psubscribe(patterns, state)

  def dispatch_connection_ast({:punsubscribe, patterns}, state),
    do: ConnPubSub.dispatch_punsubscribe(patterns, state)

  def dispatch_connection_ast(:multi, state), do: ConnTransaction.dispatch_multi([], state)

  def dispatch_connection_ast({:multi, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast(:exec, state), do: ConnTransaction.dispatch_exec([], state)

  def dispatch_connection_ast({:exec, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast(:discard, state), do: ConnTransaction.dispatch_discard([], state)

  def dispatch_connection_ast({:discard, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast({:watch, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}

  def dispatch_connection_ast({:watch, keys}, state),
    do: ConnTransaction.dispatch_watch(keys, state)

  def dispatch_connection_ast(:unwatch, state), do: ConnTransaction.dispatch_unwatch([], state)

  def dispatch_connection_ast({:unwatch, {:error, _} = err}, state),
    do: {:continue, Encoder.encode(err), state}
end
