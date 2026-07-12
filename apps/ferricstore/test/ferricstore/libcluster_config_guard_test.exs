defmodule Ferricstore.LibclusterConfigGuardTest do
  use ExUnit.Case, async: true

  @config_path Path.expand("../../../../config/config.exs", __DIR__)
  @runtime_path Path.expand("../../../../config/runtime.exs", __DIR__)

  test "base config does not start libcluster gossip on all interfaces by default" do
    source = File.read!(@config_path)

    assert source =~ "config :libcluster"
    assert source =~ "topologies: :disabled"
    refute source =~ "strategy: Cluster.Strategy.Gossip"
    refute source =~ ~s(if_addr: "0.0.0.0")
    refute source =~ ~s(multicast_if: "0.0.0.0")
  end

  test "runtime gossip discovery defaults to loopback bind" do
    source = File.read!(@runtime_path)

    assert source =~ ~S|System.get_env("FERRICSTORE_GOSSIP_IF_ADDR", "127.0.0.1")|
    assert source =~ ~S|System.get_env("FERRICSTORE_GOSSIP_MULTICAST_IF", gossip_if_addr)|
    refute source =~ ~S|System.get_env("FERRICSTORE_GOSSIP_IF_ADDR", "0.0.0.0")|
    refute source =~ ~S|System.get_env("FERRICSTORE_GOSSIP_MULTICAST_IF", "0.0.0.0")|
  end
end
