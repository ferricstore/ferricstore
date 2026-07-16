defmodule Ferricstore.Raft.WARaftPersistedConfigTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Raft.WARaftStorage

  @peer_tag "ferricstore_waraft_peer"

  test "canonical persisted peers round trip configured node names" do
    configured_node = node()
    node_name = Atom.to_string(configured_node)
    config = %{participants: [{@peer_tag, Atom.to_string(__MODULE__), node_name}]}

    assert {:ok, %{participants: [{__MODULE__, ^configured_node}]}} =
             WARaftStorage.__decode_persisted_waraft_config_for_test__(config)
  end

  test "unknown persisted node names are rejected without interning atoms" do
    node_name = unique_node_name("unknown-node")
    refute existing_atom?(node_name)

    result =
      WARaftStorage.__decode_persisted_waraft_config_for_test__(%{
        participants: [{@peer_tag, Atom.to_string(__MODULE__), node_name}]
      })

    refute existing_atom?(node_name)
    assert {:error, {:unknown_persisted_peer_node, ^node_name}} = result
  end

  test "unknown persisted server names are rejected without interning atoms" do
    server_name = "unknown_server_#{System.unique_integer([:positive, :monotonic])}"
    node_name = unique_node_name("unknown-server")
    refute existing_atom?(server_name)

    result =
      WARaftStorage.__decode_persisted_waraft_config_for_test__(%{
        membership: [{@peer_tag, server_name, node_name}]
      })

    refute existing_atom?(server_name)
    assert {:error, {:unknown_persisted_peer_server, ^server_name}} = result
  end

  test "peer cardinality is rejected before any node atoms are created" do
    suffix = System.unique_integer([:positive, :monotonic])

    node_names =
      Enum.map(1..65, fn index -> "peer-#{suffix}-#{index}@host" end)

    refute existing_atom?(hd(node_names))
    refute existing_atom?(List.last(node_names))

    peers =
      Enum.map(node_names, fn node_name ->
        {@peer_tag, Atom.to_string(__MODULE__), node_name}
      end)

    result =
      WARaftStorage.__decode_persisted_waraft_config_for_test__(%{participants: peers})

    refute existing_atom?(hd(node_names))
    refute existing_atom?(List.last(node_names))
    assert {:error, {:too_many_persisted_peers, 65, 64}} = result
  end

  test "untagged peer tuples are rejected instead of treated as another wire shape" do
    node_name = node()

    assert {:error, {:invalid_persisted_peer, {__MODULE__, ^node_name}}} =
             WARaftStorage.__decode_persisted_waraft_config_for_test__(%{
               participants: [{__MODULE__, node_name}]
             })
  end

  defp unique_node_name(prefix) do
    "#{prefix}-#{System.unique_integer([:positive, :monotonic])}@host"
  end

  defp existing_atom?(name) do
    _ = String.to_existing_atom(name)
    true
  rescue
    ArgumentError -> false
  end
end
