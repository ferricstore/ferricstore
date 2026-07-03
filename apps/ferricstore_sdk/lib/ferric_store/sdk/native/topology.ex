defmodule FerricStore.SDK.Native.Topology do
  @moduledoc false

  import Bitwise

  @num_slots 1024
  @slot_mask @num_slots - 1

  defstruct route_epoch: 0,
            shard_count: 0,
            slots: List.to_tuple(List.duplicate(nil, @num_slots)),
            endpoints: %{}

  @type t :: %__MODULE__{}

  @type endpoint :: %{
          required(:node) => binary(),
          required(:host) => binary(),
          required(:native_port) => non_neg_integer(),
          optional(:native_tls_port) => non_neg_integer()
        }

  @spec build(map()) :: {:ok, struct()} | {:error, term()}
  def build(%{"ranges" => ranges} = payload) when is_list(ranges) do
    topology =
      %__MODULE__{
        route_epoch: int(payload["route_epoch"], 0),
        shard_count: int(payload["shard_count"], 0)
      }

    Enum.reduce_while(ranges, {:ok, topology}, fn range, {:ok, acc} ->
      case put_range(acc, range) do
        {:ok, next} -> {:cont, {:ok, next}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  def build(%{ranges: ranges} = payload) when is_list(ranges) do
    payload
    |> stringify_keys()
    |> build()
  end

  def build(_payload), do: {:error, :invalid_shards_payload}

  @spec route_key(struct(), binary()) :: {:ok, map()} | {:error, term()}
  def route_key(%__MODULE__{} = topology, key) when is_binary(key) do
    slot = slot_for_key(key)

    case elem(topology.slots, slot) do
      nil -> {:error, {:unmapped_slot, slot}}
      route -> {:ok, Map.put(route, :slot, slot)}
    end
  end

  @spec slot_for_key(binary()) :: non_neg_integer()
  def slot_for_key("f:{" <> rest = key), do: slot_for_flow_tag(rest, key)
  def slot_for_key("X:f:{" <> rest = key), do: slot_for_flow_tag(rest, key)

  def slot_for_key(key) when is_binary(key) do
    key
    |> extract_hash_tag_or_key()
    |> slot_for_hash_input()
  end

  @spec endpoint_key(endpoint()) :: {binary(), non_neg_integer()}
  def endpoint_key(%{host: host, native_port: port}), do: {host, port}

  defp put_range(topology, range) do
    with {:ok, first} <- fetch_int(range, "first_slot"),
         {:ok, last} <- fetch_int(range, "last_slot"),
         {:ok, shard} <- fetch_int(range, "shard"),
         {:ok, lane_id} <- fetch_int(range, "lane_id"),
         {:ok, endpoint} <- endpoint_from_range(range),
         true <- first in 0..(@num_slots - 1),
         true <- last in first..(@num_slots - 1) do
      key = endpoint_key(endpoint)

      route = %{
        shard: shard,
        lane_id: lane_id,
        endpoint_key: key,
        endpoint: endpoint,
        leader_node: endpoint.node
      }

      slots =
        Enum.reduce(first..last, topology.slots, fn slot, acc ->
          put_elem(acc, slot, route)
        end)

      {:ok, %{topology | slots: slots, endpoints: Map.put(topology.endpoints, key, endpoint)}}
    else
      _ -> {:error, {:invalid_range, range}}
    end
  end

  defp endpoint_from_range(%{"endpoint" => endpoint}) when is_map(endpoint),
    do: endpoint_from_map(endpoint)

  defp endpoint_from_range(range), do: endpoint_from_map(range)

  defp endpoint_from_map(map) do
    with host when is_binary(host) <- map["host"] || map["native_host"],
         port when is_integer(port) <- map["native_port"] do
      endpoint =
        %{
          node: map["node"] || map["leader_node"] || map["owner_node"] || host,
          host: host,
          native_port: port
        }
        |> maybe_put(:native_tls_port, map["native_tls_port"])

      {:ok, endpoint}
    else
      _ -> {:error, :invalid_endpoint}
    end
  end

  defp fetch_int(map, key) do
    case map[key] do
      value when is_integer(value) -> {:ok, value}
      _ -> {:error, key}
    end
  end

  defp int(value, _default) when is_integer(value), do: value
  defp int(_value, default), do: default

  defp slot_for_flow_tag(rest, fallback_key) do
    case :binary.match(rest, "}") do
      {end_pos, 1} when end_pos > 0 ->
        rest
        |> binary_part(0, end_pos)
        |> slot_for_hash_input()

      _ ->
        fallback_key
        |> extract_hash_tag_or_key()
        |> slot_for_hash_input()
    end
  end

  defp extract_hash_tag_or_key(key) do
    case :binary.match(key, "{") do
      {start, 1} ->
        after_open = start + 1

        case :binary.match(binary_part(key, after_open, byte_size(key) - after_open), "}") do
          {end_rel, 1} when end_rel > 0 -> binary_part(key, after_open, end_rel)
          _ -> key
        end

      :nomatch ->
        key
    end
  end

  defp slot_for_hash_input(hash_input), do: :erlang.crc32(hash_input) |> band(@slot_mask)

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), stringify_keys(value)}
      {key, value} -> {key, stringify_keys(value)}
    end)
  end

  defp stringify_keys(values) when is_list(values), do: Enum.map(values, &stringify_keys/1)
  defp stringify_keys(value), do: value

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
