defmodule FerricstoreServer.Native.RouteMetadata do
  @moduledoc false

  alias Ferricstore.Raft.Cluster, as: RaftCluster
  alias FerricstoreServer.Native.{Listener, TlsListener}

  @endpoint_lookup_timeout_ms 1_000

  @spec endpoint() :: map()
  def endpoint, do: local_endpoint(node())

  @spec target_for_shard(non_neg_integer()) :: map()
  def target_for_shard(shard_index) do
    case leader_node_for_shard(shard_index) do
      leader_node when is_atom(leader_node) and not is_nil(leader_node) ->
        endpoint = endpoint_for_node(leader_node)
        hint = if leader_node == node(), do: "leader", else: "remote_leader"
        target_payload(endpoint, hint)

      _unknown ->
        unknown_target_payload()
    end
  end

  @spec endpoint_for_node(node()) :: map()
  def endpoint_for_node(node_name) when node_name == node(), do: local_endpoint(node_name)

  def endpoint_for_node(node_name) when is_atom(node_name) do
    try do
      case :erpc.call(node_name, __MODULE__, :endpoint, [], @endpoint_lookup_timeout_ms) do
        endpoint when is_map(endpoint) -> normalize_endpoint(endpoint, node_name)
        _other -> fallback_endpoint(node_name)
      end
    catch
      _kind, _reason -> fallback_endpoint(node_name)
    end
  end

  defp leader_node_for_shard(shard_index) do
    case RaftCluster.members(shard_index, 0) do
      {:ok, _members, {_server, leader_node}} when is_atom(leader_node) -> leader_node
      {:ok, _members, leader_node} when is_atom(leader_node) -> leader_node
      _other -> nil
    end
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp target_payload(endpoint, hint) do
    %{
      owner_node: endpoint.node,
      leader_node: endpoint.node,
      native_host: endpoint.host,
      native_port: endpoint.native_port,
      endpoint: endpoint,
      hint: hint
    }
    |> maybe_put(:native_tls_port, Map.get(endpoint, :native_tls_port))
  end

  defp unknown_target_payload do
    %{
      owner_node: nil,
      leader_node: nil,
      hint: "leader_unknown"
    }
  end

  defp local_endpoint(node_name) do
    %{
      node: Atom.to_string(node_name),
      host: advertised_host(node_name),
      native_port: advertised_native_port()
    }
    |> maybe_put(:native_tls_port, advertised_native_tls_port())
  end

  defp fallback_endpoint(node_name) do
    %{
      node: Atom.to_string(node_name),
      host: fallback_host(node_name),
      native_port: fallback_native_port()
    }
    |> maybe_put(:native_tls_port, Application.get_env(:ferricstore, :native_advertise_tls_port))
  end

  defp normalize_endpoint(endpoint, fallback_node) do
    node = Map.get(endpoint, :node) || Map.get(endpoint, "node") || Atom.to_string(fallback_node)
    host = Map.get(endpoint, :host) || Map.get(endpoint, "host") || advertised_host(fallback_node)

    native_port =
      Map.get(endpoint, :native_port) || Map.get(endpoint, "native_port") ||
        fallback_native_port()

    %{
      node: node,
      host: host,
      native_port: native_port
    }
    |> maybe_put(
      :native_tls_port,
      Map.get(endpoint, :native_tls_port) || Map.get(endpoint, "native_tls_port")
    )
  end

  defp advertised_host(node_name) do
    Application.get_env(:ferricstore, :native_advertise_host) ||
      host_from_node_name(node_name)
  end

  defp fallback_host(node_name) do
    if node_name == node() do
      advertised_host(node_name)
    else
      host_from_node_name(node_name)
    end
  end

  defp host_from_node_name(node_name) do
    case node_name |> Atom.to_string() |> String.split("@", parts: 2) do
      [_name, host] when host not in ["", "nohost"] -> host
      _other -> "127.0.0.1"
    end
  end

  defp advertised_native_port do
    Application.get_env(:ferricstore, :native_advertise_port) ||
      listener_port(Listener) ||
      fallback_native_port()
  end

  defp advertised_native_tls_port do
    Application.get_env(:ferricstore, :native_advertise_tls_port) ||
      listener_port(TlsListener) ||
      Application.get_env(:ferricstore, :native_tls_port)
  end

  defp fallback_native_port do
    Application.get_env(:ferricstore, :native_advertise_port) ||
      Application.get_env(:ferricstore, :native_port, 6388)
  end

  defp listener_port(listener) do
    if listener.running?(), do: listener.port()
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
