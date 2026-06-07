Code.require_file("manager_test/sections/part_01.exs", __DIR__)
Code.require_file("manager_test/sections/part_02.exs", __DIR__)

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

  use Ferricstore.Cluster.ManagerTest.Sections.Part01

  use Ferricstore.Cluster.ManagerTest.Sections.Part02
end
