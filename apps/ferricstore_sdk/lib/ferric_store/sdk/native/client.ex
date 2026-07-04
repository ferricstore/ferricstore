defmodule FerricStore.SDK.Native.Client do
  @moduledoc """
  Minimal FerricStore native protocol client with topology-aware shard routing.
  """

  use GenServer

  alias FerricStore.SDK.Native.{Connection, Opcodes, Topology}

  @op_hello 0x0001
  @op_auth 0x0002
  @op_ping 0x0003
  @op_shards 0x0007
  @op_command_exec 0x0100
  @op_get 0x0101
  @op_set 0x0102
  @control_lane_opcodes MapSet.new([
                          0x0001,
                          0x0002,
                          0x0003,
                          0x0004,
                          0x0005,
                          0x0006,
                          0x0007,
                          0x0008,
                          0x0009,
                          0x000B,
                          0x000C,
                          0x000D,
                          0x000F,
                          0x0011,
                          0x0012
                        ])
  @default_timeout 5_000
  @endpoint_option_keys [
    :server_name,
    :verify,
    :tls_verify,
    :cacertfile,
    :cacerts,
    :connect_timeout
  ]

  defstruct [
    :username,
    :password,
    :tls,
    :server_name,
    :client_name,
    :endpoint_validator,
    :endpoint_policy,
    :trusted_hosts,
    :endpoint_options,
    :warm_connections,
    :topology,
    seeds: [],
    connections: %{}
  ]

  @type seed :: {binary(), non_neg_integer()} | map()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @spec close(pid()) :: :ok
  def close(client), do: GenServer.stop(client, :normal)

  @spec from_url(binary(), keyword()) :: GenServer.on_start()
  def from_url(url, opts \\ []) do
    with {:ok, seed, tls} <- parse_url(url) do
      start_link(Keyword.merge(opts, seeds: [seed], tls: tls))
    end
  end

  @spec get(pid(), binary(), keyword()) :: {:ok, binary() | nil} | {:error, term()}
  def get(client, key, opts \\ []),
    do: request_by_key(client, @op_get, key, %{"key" => key}, opts)

  @spec set(pid(), binary(), term(), keyword()) :: :ok | {:ok, term()} | {:error, term()}
  def set(client, key, value, opts \\ []) do
    payload =
      %{"key" => key, "value" => value}
      |> maybe_put("ttl", Keyword.get(opts, :ttl))
      |> maybe_put("nx", Keyword.get(opts, :nx))
      |> maybe_put("xx", Keyword.get(opts, :xx))
      |> maybe_put("get", Keyword.get(opts, :get))
      |> maybe_put("keepttl", Keyword.get(opts, :keepttl))
      |> maybe_put("exat", Keyword.get(opts, :exat))
      |> maybe_put("pxat", Keyword.get(opts, :pxat))

    case request_by_key(client, @op_set, key, payload, opts) do
      {:ok, "OK"} -> :ok
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec ping(pid(), term(), keyword()) :: {:ok, term()} | {:error, term()}
  def ping(client, message \\ "PONG", opts \\ []) do
    request(client, @op_ping, %{"message" => message}, opts)
  end

  @spec command_exec(pid(), binary(), list(), keyword()) :: {:ok, term()} | {:error, term()}
  def command_exec(client, command, args \\ [], opts \\ []) when is_list(args) do
    payload =
      %{"command" => command, "args" => args}
      |> maybe_put("request_context", Keyword.get(opts, :request_context))

    case Keyword.get(opts, :key) do
      key when is_binary(key) -> request_by_key(client, @op_command_exec, key, payload, opts)
      _other -> request(client, @op_command_exec, payload, opts)
    end
  end

  @spec request(pid(), non_neg_integer() | atom() | binary(), map(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def request(client, opcode, payload \\ %{}, opts \\ []) do
    with {:ok, opcode} <- Opcodes.fetch(opcode) do
      GenServer.call(client, {:request, opcode, payload || %{}, opts}, call_timeout(opts))
    end
  end

  @spec request_by_key(pid(), non_neg_integer() | atom() | binary(), binary(), map(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def request_by_key(client, opcode, key, payload, opts \\ []) do
    with {:ok, opcode} <- Opcodes.fetch(opcode) do
      GenServer.call(client, {:command, opcode, key, payload || %{}, opts}, call_timeout(opts))
    end
  end

  @spec request_by_keys(
          pid(),
          non_neg_integer() | atom() | binary(),
          [binary()],
          ([binary()] -> map()),
          keyword()
        ) :: {:ok, [map()]} | {:error, term()}
  def request_by_keys(client, opcode, keys, payload_builder, opts \\ [])
      when is_list(keys) and is_function(payload_builder, 1) do
    request_by_items(client, opcode, keys, & &1, payload_builder, opts)
  end

  @spec request_by_items(
          pid(),
          non_neg_integer() | atom() | binary(),
          list(),
          (term() -> binary()),
          (list() -> map()),
          keyword()
        ) :: {:ok, [map()]} | {:error, term()}
  def request_by_items(client, opcode, items, key_fun, payload_builder, opts \\ [])
      when is_list(items) and is_function(key_fun, 1) and is_function(payload_builder, 1) do
    with {:ok, opcode} <- Opcodes.fetch(opcode) do
      GenServer.call(
        client,
        {:command_items, opcode, items, key_fun, payload_builder, opts},
        call_timeout(opts)
      )
    end
  end

  @spec topology(pid()) :: Topology.t()
  def topology(client), do: GenServer.call(client, :topology)

  @spec route(pid(), binary()) :: {:ok, map()} | {:error, term()}
  def route(client, key), do: GenServer.call(client, {:route, key})

  @spec refresh_topology(pid()) :: :ok | {:error, term()}
  def refresh_topology(client), do: GenServer.call(client, :refresh_topology)

  @impl true
  def init(opts) do
    endpoint_options = endpoint_options(opts)

    seeds =
      normalize_seeds(
        Keyword.fetch!(opts, :seeds),
        Keyword.get(opts, :tls, false),
        endpoint_options
      )

    state = %__MODULE__{
      seeds: seeds,
      username: Keyword.get(opts, :username),
      password: Keyword.get(opts, :password),
      tls: Keyword.get(opts, :tls, false),
      server_name: Keyword.get(opts, :server_name),
      endpoint_validator: Keyword.get(opts, :endpoint_validator),
      endpoint_policy: Keyword.get(opts, :endpoint_policy, :seed_hosts),
      trusted_hosts: trusted_hosts(seeds, opts),
      endpoint_options: endpoint_options,
      warm_connections: Keyword.get(opts, :warm_connections, false),
      client_name: Keyword.get(opts, :client_name, "ferricstore-elixir-sdk")
    }

    case refresh_topology_state(state) do
      {:ok, state} -> {:ok, state}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:topology, _from, state), do: {:reply, state.topology, state}

  def handle_call(:refresh_topology, _from, state) do
    case refresh_topology_state(state) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:route, key}, _from, state),
    do: {:reply, Topology.route_key(state.topology, key), state}

  def handle_call({:request, opcode, payload, opts}, _from, state) do
    case control_request(state, opcode, payload, opts) do
      {:ok, value, next_state} ->
        {:reply, {:ok, value}, next_state}

      {:error, reason, next_state} ->
        {:reply, {:error, reason}, next_state}
    end
  end

  def handle_call({:command, opcode, key, payload, opts}, from, state) do
    case maybe_async_routed_request(state, from, opcode, key, payload, opts) do
      :ok ->
        {:noreply, state}

      :fallback ->
        case routed_request(state, opcode, key, payload, opts) do
          {:ok, value, next_state} ->
            {:reply, {:ok, value}, next_state}

          {:error, reason, next_state} ->
            {:reply, {:error, reason}, next_state}
        end
    end
  end

  def handle_call({:retry_command, opcode, key, payload, opts, original_reason}, _from, state) do
    case retry_after_refresh(state, opcode, key, payload, opts, original_reason) do
      {:ok, value, next_state} ->
        {:reply, {:ok, value}, next_state}

      {:error, reason, next_state} ->
        {:reply, {:error, reason}, next_state}
    end
  end

  def handle_call({:command_items, opcode, items, key_fun, payload_builder, opts}, _from, state) do
    case routed_requests_by_items(state, opcode, items, key_fun, payload_builder, opts) do
      {:ok, values, next_state} ->
        {:reply, {:ok, values}, next_state}

      {:error, reason, next_state} ->
        {:reply, {:error, reason}, next_state}
    end
  end

  defp control_request(state, opcode, payload, opts) do
    endpoint = Keyword.get(opts, :endpoint) || control_endpoint(state)
    lane_id = Keyword.get(opts, :lane_id, default_lane_id(opcode))

    with {:ok, conn, state} <- ensure_connection(state, endpoint),
         {:ok, value} <-
           Connection.request(
             conn,
             opcode,
             payload,
             lane_id,
             Keyword.get(opts, :timeout, @default_timeout)
           ) do
      {:ok, value, state}
    else
      {:error, reason} when reason in [:closed, :timeout, :econnrefused] ->
        retry_control_after_refresh(state, opcode, payload, opts, reason)

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp retry_control_after_refresh(state, opcode, payload, opts, original_reason) do
    endpoint = Keyword.get(opts, :endpoint)
    lane_id = Keyword.get(opts, :lane_id, default_lane_id(opcode))

    with {:ok, state} <- refresh_topology_state(state),
         endpoint <- endpoint || control_endpoint(state),
         {:ok, conn, state} <- ensure_connection(state, endpoint),
         {:ok, value} <-
           Connection.request(
             conn,
             opcode,
             payload,
             lane_id,
             Keyword.get(opts, :timeout, @default_timeout)
           ) do
      {:ok, value, state}
    else
      {:error, reason} -> {:error, {:retry_failed, original_reason, reason}, state}
    end
  end

  defp maybe_async_routed_request(state, from, opcode, key, payload, opts) do
    with {:ok, route} <- Topology.route_key(state.topology, key),
         {:ok, conn} <- Map.fetch(state.connections, route.endpoint_key),
         true <- Process.alive?(conn) do
      client = self()
      timeout = Keyword.get(opts, :timeout, @default_timeout)

      case Task.start(fn ->
             result =
               async_connection_request(
                 client,
                 conn,
                 opcode,
                 key,
                 payload,
                 route.lane_id,
                 timeout,
                 opts
               )

             GenServer.reply(from, result)
           end) do
        {:ok, _pid} -> :ok
        {:error, _reason} -> :fallback
      end
    else
      _other -> :fallback
    end
  end

  defp async_connection_request(client, conn, opcode, key, payload, lane_id, timeout, opts) do
    case safe_connection_request(conn, opcode, payload, lane_id, timeout) do
      {:error, reason} when reason in [:closed, :timeout, :econnrefused] ->
        retry_command_call(client, opcode, key, payload, opts, reason)

      {:error, {:reroute, _payload} = reason} ->
        retry_command_call(client, opcode, key, payload, opts, reason)

      result ->
        result
    end
  end

  defp safe_connection_request(conn, opcode, payload, lane_id, timeout) do
    Connection.request(conn, opcode, payload, lane_id, timeout)
  catch
    :exit, reason -> {:error, {:connection_call_exit, reason}}
  end

  defp retry_command_call(client, opcode, key, payload, opts, original_reason) do
    GenServer.call(
      client,
      {:retry_command, opcode, key, payload, opts, original_reason},
      call_timeout(opts)
    )
  catch
    :exit, reason -> {:error, {:retry_call_exit, reason}}
  end

  defp routed_request(state, opcode, key, payload, opts) do
    with {:ok, route} <- Topology.route_key(state.topology, key),
         {:ok, conn, state} <- ensure_connection(state, route.endpoint),
         {:ok, value} <-
           Connection.request(
             conn,
             opcode,
             payload,
             route.lane_id,
             Keyword.get(opts, :timeout, @default_timeout)
           ) do
      {:ok, value, state}
    else
      {:error, reason} when reason in [:closed, :timeout, :econnrefused] ->
        retry_after_refresh(state, opcode, key, payload, opts, reason)

      {:error, {:reroute, _payload} = reason} ->
        retry_after_refresh(state, opcode, key, payload, opts, reason)

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp retry_after_refresh(state, opcode, key, payload, opts, original_reason) do
    with {:ok, state} <- refresh_topology_state(state),
         {:ok, route} <- Topology.route_key(state.topology, key),
         {:ok, conn, state} <- ensure_connection(state, route.endpoint),
         {:ok, value} <-
           Connection.request(
             conn,
             opcode,
             payload,
             route.lane_id,
             Keyword.get(opts, :timeout, @default_timeout)
           ) do
      {:ok, value, state}
    else
      {:error, reason} -> {:error, {:retry_failed, original_reason, reason}, state}
    end
  end

  defp routed_requests_by_items(state, _opcode, [], _key_fun, _payload_builder, _opts),
    do: {:ok, [], state}

  defp routed_requests_by_items(state, opcode, items, key_fun, payload_builder, opts) do
    with {:ok, groups} <- route_item_groups(state, items, key_fun),
         {:ok, groups_with_connections, state} <- ensure_group_connections(state, groups) do
      execute_group_requests(state, opcode, groups_with_connections, payload_builder, opts)
    else
      {:error, reason} when reason in [:closed, :timeout, :econnrefused] ->
        retry_items_after_refresh(state, opcode, items, key_fun, payload_builder, opts, reason)

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp retry_items_after_refresh(
         state,
         opcode,
         items,
         key_fun,
         payload_builder,
         opts,
         original_reason
       ) do
    with {:ok, state} <- refresh_topology_state(state),
         {:ok, groups} <- route_item_groups(state, items, key_fun),
         {:ok, groups_with_connections, state} <- ensure_group_connections(state, groups),
         {:ok, values, state} <-
           execute_group_requests(state, opcode, groups_with_connections, payload_builder, opts) do
      {:ok, values, state}
    else
      {:error, reason} -> {:error, {:retry_failed, original_reason, reason}, state}
    end
  end

  defp route_item_groups(state, items, key_fun) do
    items
    |> Enum.with_index()
    |> Enum.reduce_while(%{}, fn {item, index}, acc ->
      case key_fun.(item) do
        key when is_binary(key) ->
          case Topology.route_key(state.topology, key) do
            {:ok, route} ->
              group_key = {route.endpoint_key, route.lane_id}
              group = Map.get(acc, group_key, %{route: route, items: [], indexes: []})

              {:cont,
               Map.put(acc, group_key, %{
                 group
                 | items: [item | group.items],
                   indexes: [index | group.indexes]
               })}

            {:error, reason} ->
              {:halt, {:error, reason}}
          end

        other ->
          {:halt, {:error, {:invalid_route_key, other}}}
      end
    end)
    |> case do
      {:error, reason} ->
        {:error, reason}

      groups ->
        groups =
          groups
          |> Map.values()
          |> Enum.map(fn group ->
            %{group | items: Enum.reverse(group.items), indexes: Enum.reverse(group.indexes)}
          end)
          |> Enum.sort_by(fn %{indexes: [index | _]} -> index end)

        {:ok, groups}
    end
  end

  defp ensure_group_connections(state, groups) do
    Enum.reduce_while(groups, {:ok, [], state}, fn group, {:ok, acc, acc_state} ->
      case ensure_connection(acc_state, group.route.endpoint) do
        {:ok, conn, next_state} ->
          {:cont, {:ok, [Map.put(group, :conn, conn) | acc], next_state}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, groups, state} -> {:ok, Enum.reverse(groups), state}
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute_group_requests(state, opcode, groups, payload_builder, opts) do
    Enum.reduce_while(groups, {:ok, [], state}, fn group, {:ok, acc, acc_state} ->
      payload = payload_builder.(group.items)

      case Connection.request(
             group.conn,
             opcode,
             payload || %{},
             group.route.lane_id,
             Keyword.get(opts, :timeout, @default_timeout)
           ) do
        {:ok, value} ->
          result = Map.take(group, [:route, :items, :indexes]) |> Map.put(:value, value)
          {:cont, {:ok, [result | acc], acc_state}}

        {:error, reason} ->
          {:halt, {:error, reason, acc_state}}
      end
    end)
    |> case do
      {:ok, values, state} -> {:ok, Enum.reverse(values), state}
      {:error, reason, state} -> {:error, reason, state}
    end
  end

  defp refresh_topology_state(state) do
    Enum.reduce_while(refresh_candidates(state), {:error, :no_endpoint_reachable}, fn seed,
                                                                                      _acc ->
      case connect_and_bootstrap(state, seed) do
        {:ok, topology, conn} ->
          key = Topology.endpoint_key(seed)

          state =
            %{state | topology: topology, connections: Map.put(state.connections, key, conn)}
            |> maybe_warm_topology_connections()

          {:halt, {:ok, state}}

        {:error, _reason} = error ->
          {:cont, error}
      end
    end)
  end

  defp connect_and_bootstrap(state, endpoint) do
    with :ok <- validate_endpoint(state, endpoint),
         {:ok, conn} <- Connection.start(endpoint),
         {:ok, _hello} <- hello(conn, state),
         :ok <- maybe_auth(conn, state),
         {:ok, shards} <- Connection.request(conn, @op_shards, %{}, 0),
         {:ok, topology} <- Topology.build(shards) do
      {:ok, topology, conn}
    end
  end

  defp control_endpoint(%{topology: %Topology{endpoints: endpoints}})
       when map_size(endpoints) > 0 do
    endpoints
    |> Map.values()
    |> List.first()
  end

  defp control_endpoint(%{seeds: [seed | _]}), do: seed

  defp control_endpoint(%{seeds: []}), do: %{host: "127.0.0.1", native_port: 6379}

  defp refresh_candidates(state) do
    topology_endpoints =
      case state.topology do
        %Topology{endpoints: endpoints} -> Map.values(endpoints)
        _other -> []
      end

    (state.seeds ++ topology_endpoints)
    |> Enum.map(&endpoint_defaults(&1, state))
    |> Enum.uniq_by(&Topology.endpoint_key/1)
  end

  defp default_lane_id(opcode) do
    if MapSet.member?(@control_lane_opcodes, opcode), do: 0, else: 1
  end

  defp hello(conn, state) do
    Connection.request(conn, @op_hello, %{"client_name" => state.client_name}, 0)
  end

  defp maybe_auth(_conn, %{username: nil}), do: :ok
  defp maybe_auth(_conn, %{password: nil}), do: :ok

  defp maybe_auth(conn, state) do
    case Connection.request(
           conn,
           @op_auth,
           %{"username" => state.username, "password" => state.password},
           0
         ) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_connection(state, endpoint) do
    key = Topology.endpoint_key(endpoint)

    case Map.fetch(state.connections, key) do
      {:ok, conn} when is_pid(conn) ->
        if Process.alive?(conn) do
          {:ok, conn, state}
        else
          start_connection(state, key, endpoint)
        end

      :error ->
        start_connection(state, key, endpoint)
    end
  end

  defp start_connection(state, key, endpoint) do
    endpoint = endpoint_defaults(endpoint, state)

    with :ok <- validate_endpoint(state, endpoint),
         {:ok, conn} <- Connection.start(endpoint),
         {:ok, _hello} <- hello(conn, state),
         :ok <- maybe_auth(conn, state) do
      {:ok, conn, %{state | connections: Map.put(state.connections, key, conn)}}
    end
  end

  defp maybe_warm_topology_connections(%{warm_connections: true} = state),
    do: warm_topology_connections(state)

  defp maybe_warm_topology_connections(state), do: state

  defp warm_topology_connections(state) do
    Enum.reduce(state.topology.endpoints, state, fn {_key, endpoint}, acc ->
      case ensure_connection(acc, endpoint) do
        {:ok, _conn, next} -> next
        {:error, _reason} -> acc
      end
    end)
  end

  defp endpoint_defaults(endpoint, state) do
    endpoint
    |> Map.put_new(:tls, state.tls)
    |> apply_endpoint_options(state.endpoint_options || %{})
    |> put_if_present(:server_name, state.server_name)
  end

  defp validate_endpoint(state, endpoint) do
    with :ok <- validate_endpoint_policy(state, endpoint) do
      validate_endpoint_validator(state, endpoint)
    end
  end

  defp validate_endpoint_policy(%{endpoint_policy: :any}, _endpoint), do: :ok
  defp validate_endpoint_policy(%{endpoint_policy: :none}, _endpoint), do: :ok

  defp validate_endpoint_policy(
         %{endpoint_policy: :seed_hosts, trusted_hosts: trusted_hosts},
         endpoint
       ) do
    if trusted_endpoint_host?(trusted_hosts, endpoint) do
      :ok
    else
      {:error, :unsafe_endpoint}
    end
  end

  defp validate_endpoint_policy(%{endpoint_policy: {:allow_hosts, hosts}}, endpoint) do
    trusted_hosts = hosts |> List.wrap() |> normalized_host_set()

    if trusted_endpoint_host?(trusted_hosts, endpoint) do
      :ok
    else
      {:error, :unsafe_endpoint}
    end
  end

  defp validate_endpoint_policy(%{endpoint_policy: other}, _endpoint),
    do: {:error, {:invalid_endpoint_policy, other}}

  defp validate_endpoint_validator(%{endpoint_validator: nil}, _endpoint), do: :ok

  defp validate_endpoint_validator(%{endpoint_validator: validator}, endpoint)
       when is_function(validator, 1) do
    case validator.(endpoint) do
      :ok -> :ok
      {:ok, _value} -> :ok
      true -> :ok
      false -> {:error, :unsafe_endpoint}
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_endpoint_validator_result, other}}
    end
  end

  defp validate_endpoint_validator(_state, _endpoint), do: {:error, :invalid_endpoint_validator}

  defp normalize_seeds(seeds, tls, endpoint_options) do
    Enum.map(seeds, fn
      {host, port} ->
        %{node: host, host: host, native_port: port, tls: tls}
        |> apply_endpoint_options(endpoint_options)

      %{host: host, native_port: _port} = seed ->
        seed
        |> Map.put_new(:node, host)
        |> Map.put_new(:tls, tls)
        |> apply_endpoint_options(endpoint_options)

      %{"host" => host, "native_port" => port} = seed ->
        %{
          node: seed["node"] || host,
          host: host,
          native_port: port,
          native_tls_port: seed["native_tls_port"],
          tls: tls,
          server_name: seed["server_name"]
        }
        |> Enum.reject(fn {_key, value} -> is_nil(value) end)
        |> Map.new()
        |> apply_endpoint_options(endpoint_options)
    end)
  end

  defp endpoint_options(opts) do
    opts
    |> Keyword.take(@endpoint_option_keys)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp apply_endpoint_options(endpoint, endpoint_options) do
    Enum.reduce(endpoint_options, endpoint, fn {key, value}, acc ->
      Map.put_new(acc, key, value)
    end)
  end

  defp trusted_hosts(seeds, opts) do
    seed_hosts = Enum.map(seeds, &Map.get(&1, :host))
    configured_hosts = Keyword.get(opts, :trusted_hosts, [])
    normalized_host_set(seed_hosts ++ List.wrap(configured_hosts))
  end

  defp trusted_endpoint_host?(trusted_hosts, endpoint) do
    host = endpoint |> Map.get(:host) |> normalize_host()
    is_binary(host) and MapSet.member?(trusted_hosts, host)
  end

  defp normalized_host_set(hosts) do
    hosts
    |> Enum.map(&normalize_host/1)
    |> Enum.reject(&is_nil/1)
    |> MapSet.new()
  end

  defp normalize_host(host) when is_binary(host) do
    host
    |> String.trim()
    |> String.downcase()
    |> case do
      "" -> nil
      normalized -> normalized
    end
  end

  defp normalize_host(host) when is_atom(host), do: host |> Atom.to_string() |> normalize_host()
  defp normalize_host(_host), do: nil

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put_new(map, key, value)

  defp parse_url(url) do
    uri = URI.parse(url)

    with {:ok, tls} <- url_tls(uri.scheme),
         true <- is_binary(uri.host),
         true <- is_integer(uri.port) do
      {:ok, {uri.host, uri.port}, tls}
    else
      false -> {:error, :invalid_url}
      {:error, _reason} = error -> error
    end
  end

  defp url_tls("ferrics"), do: {:ok, true}
  defp url_tls("ferric+tls"), do: {:ok, true}
  defp url_tls("ferric"), do: {:ok, false}
  defp url_tls(scheme) when is_binary(scheme), do: {:error, {:invalid_url_scheme, scheme}}
  defp url_tls(_scheme), do: {:error, :invalid_url}

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp call_timeout(opts),
    do: Keyword.get(opts, :call_timeout, Keyword.get(opts, :timeout, @default_timeout) + 1_000)
end
