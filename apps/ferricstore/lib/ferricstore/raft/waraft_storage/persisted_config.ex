defmodule Ferricstore.Raft.WARaftStorage.PersistedConfig do
  @moduledoc false

  @peer_tag "ferricstore_waraft_peer"
  @peer_keys [:membership, :participants, :witness, :witnesses]
  @max_peers 64
  @max_atom_bytes 255

  @spec encode(map()) :: {:ok, map()} | {:error, term()}
  def encode(config) when is_map(config) do
    with {:ok, peers} <- collect_peers(config, :runtime),
         :ok <- validate_peer_count(peers) do
      {:ok, transform_lists(config, &encode_peer/1)}
    end
  end

  def encode(other), do: {:error, {:invalid_persisted_config, other}}

  @spec decode(map()) :: {:ok, map()} | {:error, term()}
  def decode(config) when is_map(config) do
    with {:ok, peers} <- collect_peers(config, :persisted),
         :ok <- validate_peer_count(peers),
         {:ok, servers} <- resolve_servers(peers) do
      decode_lists(config, servers)
    end
  end

  def decode(other), do: {:error, {:invalid_persisted_config, other}}

  @spec encode!(map()) :: map()
  def encode!(config), do: unwrap!(encode(config))

  @spec decode!(map()) :: map()
  def decode!(config), do: unwrap!(decode(config))

  defp collect_peers(config, mode) do
    Enum.reduce_while(@peer_keys, {:ok, []}, fn key, {:ok, acc} ->
      case Map.fetch(config, key) do
        :error ->
          {:cont, {:ok, acc}}

        {:ok, nil} ->
          {:cont, {:ok, acc}}

        {:ok, peers} when is_list(peers) ->
          with :ok <- validate_peer_list_size(peers),
               :ok <- validate_peers(peers, mode) do
            {:cont, {:ok, peers ++ acc}}
          else
            {:error, _reason} = error -> {:halt, error}
          end

        {:ok, invalid} ->
          {:halt, {:error, {:invalid_persisted_peer_list, key, invalid}}}
      end
    end)
  end

  defp validate_peers(peers, mode) do
    Enum.reduce_while(peers, :ok, fn peer, :ok ->
      case validate_peer(peer, mode) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_peer_list_size(peers) do
    count = length(peers)

    if count <= @max_peers,
      do: :ok,
      else: {:error, {:too_many_persisted_peers, count, @max_peers}}
  end

  defp validate_peer({server, node_name}, :runtime)
       when is_atom(server) and is_atom(node_name) do
    server_name = Atom.to_string(server)
    node_name = Atom.to_string(node_name)

    if valid_server_name?(server_name) and valid_node_name?(node_name) do
      :ok
    else
      {:error, {:invalid_runtime_peer, {server, node_name}}}
    end
  end

  defp validate_peer({@peer_tag, server, node_name}, :persisted)
       when is_binary(server) and is_binary(node_name) do
    cond do
      not valid_server_name?(server) -> {:error, {:invalid_persisted_peer_server, server}}
      not valid_node_name?(node_name) -> {:error, {:invalid_persisted_peer_node, node_name}}
      true -> :ok
    end
  end

  defp validate_peer(peer, :runtime), do: {:error, {:invalid_runtime_peer, peer}}
  defp validate_peer(peer, :persisted), do: {:error, {:invalid_persisted_peer, peer}}

  defp validate_peer_count(peers) do
    count = peers |> Enum.map(&peer_identity/1) |> MapSet.new() |> MapSet.size()

    if count <= @max_peers,
      do: :ok,
      else: {:error, {:too_many_persisted_peers, count, @max_peers}}
  end

  defp peer_identity({server, node_name}) when is_atom(server) and is_atom(node_name),
    do: {Atom.to_string(server), Atom.to_string(node_name)}

  defp peer_identity({@peer_tag, server, node_name}), do: {server, node_name}

  defp resolve_servers(peers) do
    peers
    |> Enum.map(fn {@peer_tag, server, _node_name} -> server end)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, %{}}, fn server, {:ok, acc} ->
      case existing_atom(server) do
        {:ok, atom} -> {:cont, {:ok, Map.put(acc, server, atom)}}
        :error -> {:halt, {:error, {:unknown_persisted_peer_server, server}}}
      end
    end)
  end

  defp decode_lists(config, servers) do
    Enum.reduce_while(@peer_keys, {:ok, config}, fn key, {:ok, acc} ->
      case Map.fetch(acc, key) do
        {:ok, peers} when is_list(peers) ->
          case decode_peers(peers, servers) do
            {:ok, decoded} -> {:cont, {:ok, Map.put(acc, key, decoded)}}
            {:error, _reason} = error -> {:halt, error}
          end

        _missing_or_nil ->
          {:cont, {:ok, acc}}
      end
    end)
  end

  defp decode_peers(peers, servers) do
    Enum.reduce_while(peers, {:ok, []}, fn {@peer_tag, server, node_name}, {:ok, acc} ->
      case node_atom(node_name) do
        {:ok, node_atom} -> {:cont, {:ok, [{Map.fetch!(servers, server), node_atom} | acc]}}
        :error -> {:halt, {:error, {:invalid_persisted_peer_node, node_name}}}
      end
    end)
    |> case do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      {:error, _reason} = error -> error
    end
  end

  defp transform_lists(config, fun) do
    Enum.reduce(@peer_keys, config, fn key, acc ->
      case Map.fetch(acc, key) do
        {:ok, peers} when is_list(peers) -> Map.put(acc, key, Enum.map(peers, fun))
        _missing_or_nil -> acc
      end
    end)
  end

  defp encode_peer({server, node_name}) do
    {@peer_tag, Atom.to_string(server), Atom.to_string(node_name)}
  end

  defp valid_server_name?(name), do: valid_atom_name?(name)

  defp valid_node_name?(name) do
    valid_atom_name?(name) and
      case :binary.split(name, "@", [:global]) do
        [local, host] -> local != "" and host != ""
        _invalid -> false
      end
  end

  defp valid_atom_name?(name) do
    is_binary(name) and name != "" and byte_size(name) <= @max_atom_bytes and
      String.valid?(name) and not String.contains?(name, <<0>>)
  end

  defp existing_atom(name) do
    {:ok, String.to_existing_atom(name)}
  rescue
    ArgumentError -> :error
  end

  defp node_atom(name) do
    {:ok, String.to_atom(name)}
  rescue
    ArgumentError -> :error
  end

  defp unwrap!({:ok, value}), do: value

  defp unwrap!({:error, reason}) do
    raise ArgumentError, "invalid persisted WARaft config: #{inspect(reason)}"
  end
end
