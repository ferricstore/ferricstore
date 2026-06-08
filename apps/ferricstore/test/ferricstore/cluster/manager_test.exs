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

  # ---------------------------------------------------------------------------
  # Standalone mode
  # ---------------------------------------------------------------------------

  use Ferricstore.Cluster.ManagerTest.Sections.StandaloneMode

  use Ferricstore.Cluster.ManagerTest.Sections.RoleMembershipMappingPart2
end
