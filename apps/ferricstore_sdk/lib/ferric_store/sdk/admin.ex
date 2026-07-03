defmodule FerricStore.SDK.Admin do
  @moduledoc """
  Native FerricStore cluster and observability commands.
  """

  alias FerricStore.SDK.Native.{Client, Opcodes}

  @admin_commands [
    cluster_health: :cluster_health,
    cluster_stats: :cluster_stats,
    cluster_keyslot: :cluster_keyslot,
    cluster_slots: :cluster_slots,
    cluster_status: :cluster_status,
    cluster_join: :cluster_join,
    cluster_leave: :cluster_leave,
    cluster_failover: :cluster_failover,
    cluster_promote: :cluster_promote,
    cluster_demote: :cluster_demote,
    cluster_role: :cluster_role,
    ferricstore_key_info: :ferricstore_key_info,
    ferricstore_config: :ferricstore_config,
    ferricstore_hotness: :ferricstore_hotness,
    ferricstore_metrics: :ferricstore_metrics,
    ferricstore_blobgc: :ferricstore_blobgc
  ]

  for {function, opcode_name} <- @admin_commands do
    def unquote(function)(client, payload \\ %{}, opts \\ []) when is_map(payload) do
      request(client, unquote(opcode_name), payload, opts)
    end
  end

  @spec request(GenServer.server(), non_neg_integer() | atom() | binary(), map(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def request(client, opcode, payload \\ %{}, opts \\ []) when is_map(payload) do
    payload = stringify_keys(payload)

    case Keyword.get(opts, :route_key) || payload["key"] do
      key when is_binary(key) -> Client.request_by_key(client, opcode, key, payload, opts)
      _other -> Client.request(client, opcode, payload, opts)
    end
  end

  @spec opcodes() :: map()
  def opcodes do
    Map.new(@admin_commands, fn {function, opcode_name} ->
      {function, Opcodes.fetch!(opcode_name)}
    end)
  end

  defp stringify_keys(map) when is_map(map) do
    Map.new(map, fn
      {key, value} when is_atom(key) -> {Atom.to_string(key), stringify_keys(value)}
      {key, value} -> {key, stringify_keys(value)}
    end)
  end

  defp stringify_keys(values) when is_list(values), do: Enum.map(values, &stringify_keys/1)
  defp stringify_keys(value), do: value
end
