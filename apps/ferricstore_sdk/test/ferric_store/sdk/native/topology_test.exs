defmodule FerricStore.SDK.Native.TopologyTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.Topology

  test "uses stable crc32 slots compatible with the server" do
    assert Topology.slot_for_key("123456789") == 294
    assert Topology.slot_for_key("plain") == 719
    assert Topology.slot_for_key("{user:42}:session") == 390
    assert Topology.slot_for_key("{}empty") == 873
  end

  test "hash tags colocate related keys" do
    assert Topology.slot_for_key("{tenant:1}:a") == Topology.slot_for_key("{tenant:1}:b")
    refute Topology.slot_for_key("{tenant:1}:a") == Topology.slot_for_key("{tenant:2}:a")
  end

  test "builds slot routing table from SHARDS payload" do
    payload = %{
      "route_epoch" => 123,
      "shard_count" => 2,
      "ranges" => [
        %{
          "first_slot" => 0,
          "last_slot" => 511,
          "shard" => 0,
          "lane_id" => 1,
          "leader_node" => "a@host",
          "endpoint" => %{"node" => "a@host", "host" => "a.local", "native_port" => 6388}
        },
        %{
          "first_slot" => 512,
          "last_slot" => 1023,
          "shard" => 1,
          "lane_id" => 2,
          "leader_node" => "b@host",
          "endpoint" => %{"node" => "b@host", "host" => "b.local", "native_port" => 6388}
        }
      ]
    }

    assert {:ok, topology} = Topology.build(payload)
    assert {:ok, route0} = Topology.route_key(topology, "123456789")
    assert route0.shard == 0
    assert route0.endpoint.host == "a.local"

    assert {:ok, route1} = Topology.route_key(topology, "plain")
    assert route1.shard == 1
    assert route1.endpoint.host == "b.local"
  end
end
