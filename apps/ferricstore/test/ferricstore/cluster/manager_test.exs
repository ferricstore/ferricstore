Code.require_file("manager_test/sections/standalone_mode.exs", __DIR__)
Code.require_file("manager_test/sections/role_membership_mapping_part_2.exs", __DIR__)

defmodule Ferricstore.Cluster.ManagerTest do
  @moduledoc """
  Unit tests for Ferricstore.Cluster.Manager GenServer.

  Tests the ClusterManager in standalone mode (the default when no
  cluster_nodes are configured). The ClusterManager is already started
  by the application supervision tree; these tests call the public API
  directly.
  """

  use ExUnit.Case, async: false

  alias Ferricstore.Cluster.Manager

  setup do
    # Joins here target fake nodes with every target probe stubbed.
    Process.put(:ferricstore_cluster_manager_target_data_format_hook, fn _node ->
      {:ok, Ferricstore.DataFormat.current()}
    end)

    :ok
  end

  test "a target in another data format is refused before any other probe" do
    target = :"other_data_format_target@127.0.0.1"
    other = if Ferricstore.DataFormat.current() == 1, do: 2, else: 1

    Process.put(:ferricstore_cluster_manager_target_data_format_hook, fn ^target ->
      {:ok, other}
    end)

    Process.put(:ferricstore_cluster_manager_target_has_data_hook, fn _target, _count ->
      flunk("probed target data after a data format mismatch")
    end)

    state = %{
      mode: :cluster,
      role: :voter,
      cluster_nodes: [],
      remove_delay_ms: 60_000,
      known_nodes: MapSet.new(),
      remove_timers: %{},
      sync_status: :synced,
      shard_sync_status: %{},
      shard_count: 1
    }

    assert {:reply, {:error, {:data_format_mismatch, ^target, ^other, _current}}, ^state} =
             Manager.handle_call({:add_node, target, :voter, []}, {self(), make_ref()}, state)
  end

  # ---------------------------------------------------------------------------
  # Standalone mode
  # ---------------------------------------------------------------------------

  use Ferricstore.Cluster.ManagerTest.Sections.StandaloneMode

  use Ferricstore.Cluster.ManagerTest.Sections.RoleMembershipMappingPart2
end
