defmodule Ferricstore.LibclusterConfigGuardTest do
  use ExUnit.Case, async: true

  @config_path Path.expand("../../../../config/config.exs", __DIR__)
  @runtime_path Path.expand("../../../../config/runtime.exs", __DIR__)
  @release_env_path Path.expand("../../../../rel/env.sh.eex", __DIR__)

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

  test "runtime epmd discovery retries peers so changed DNS addresses reconnect" do
    source = File.read!(@runtime_path)

    assert source =~ ~S|System.get_env("FERRICSTORE_EPMD_POLL_INTERVAL_MS", "5000")|
    assert source =~ "strategy: Cluster.Strategy.Epmd"
  end

  test "release distribution inherits FerricStore cluster identity and cookie" do
    command =
      ~S|. "$1"; printf '%s\n%s\n%s' "$RELEASE_NODE" "$RELEASE_DISTRIBUTION" "$RELEASE_COOKIE"|

    env = [
      {"FERRICSTORE_NODE_NAME", "ferricstore@node-0.ferricstore.local"},
      {"FERRICSTORE_COOKIE", "shared-cookie"}
    ]

    assert {output, 0} = System.cmd("sh", ["-c", command, "sh", @release_env_path], env: env)

    assert output ==
             "ferricstore@node-0.ferricstore.local\nname\nshared-cookie"
  end

  test "release preserves an explicit cookie when FerricStore cookie is absent" do
    command = ~S|. "$1"; printf '%s' "$RELEASE_COOKIE"|
    env = [{"RELEASE_COOKIE", "existing-release-cookie"}]

    assert {"existing-release-cookie", 0} =
             System.cmd("sh", ["-c", command, "sh", @release_env_path], env: env)
  end
end
