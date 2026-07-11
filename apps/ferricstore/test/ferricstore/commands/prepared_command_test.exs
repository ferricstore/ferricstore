defmodule Ferricstore.Commands.PreparedCommandTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Dispatcher, KeyDiscovery, PreparedCommand}
  alias Ferricstore.Transaction.Ast, as: TransactionAst

  test "dynamic merge keys share one ACL, routing, and mutation footprint" do
    assert {:ok, prepared} =
             Dispatcher.prepare_raw("TDIGEST.MERGE", [
               "digest:dest",
               "2",
               "digest:one",
               "digest:two",
               "OVERRIDE"
             ])

    assert %PreparedCommand{
             command: "TDIGEST.MERGE",
             args: ["digest:dest", "2", "digest:one", "digest:two", "OVERRIDE"],
             ast: {:tdigest_merge, "digest:dest", ["digest:one", "digest:two"], [override: true]},
             acl_keys: ["digest:dest", "digest:one", "digest:two"],
             routing_keys: ["digest:dest", "digest:one", "digest:two"],
             read_keys: ["digest:one", "digest:two"],
             write_keys: ["digest:dest"]
           } = prepared

    assert PreparedCommand.mutation_footprint(prepared) == %{
             read: ["digest:one", "digest:two"],
             write: ["digest:dest"]
           }
  end

  test "counted and variadic dynamic commands keep read-only footprints" do
    assert {:ok, sintercard} =
             Dispatcher.prepare_raw("SINTERCARD", [
               "2",
               "set:one",
               "set:two",
               "LIMIT",
               "1"
             ])

    assert sintercard.acl_keys == ["set:one", "set:two"]
    assert sintercard.routing_keys == ["set:one", "set:two"]
    assert sintercard.read_keys == ["set:one", "set:two"]
    assert sintercard.write_keys == []

    assert {:ok, pfcount} = Dispatcher.prepare_raw("PFCOUNT", ["hll:one", "hll:two"])
    assert pfcount.acl_keys == ["hll:one", "hll:two"]
    assert pfcount.read_keys == ["hll:one", "hll:two"]
    assert pfcount.write_keys == []
  end

  test "MEMORY USAGE keeps its dynamically discovered key read-only" do
    assert {:ok, prepared} = Dispatcher.prepare_raw("MEMORY", ["USAGE", "cache:key"])

    assert prepared.acl_keys == ["cache:key"]
    assert prepared.routing_keys == ["cache:key"]
    assert prepared.read_keys == ["cache:key"]
    assert prepared.write_keys == []
  end

  test "source and destination commands describe asymmetric access" do
    assert {:ok, copy} = Dispatcher.prepare_raw("COPY", ["source", "destination"])

    assert copy.acl_keys == ["source", "destination"]
    assert copy.read_keys == ["source"]
    assert copy.write_keys == ["destination"]

    assert {:ok, rename} = Dispatcher.prepare_raw("RENAME", ["source", "destination"])
    assert rename.read_keys == ["source"]
    assert rename.write_keys == ["source", "destination"]

    assert {:ok, smove} =
             Dispatcher.prepare_raw("SMOVE", ["source", "destination", "member"])

    assert smove.read_keys == ["source", "destination"]
    assert smove.write_keys == ["source", "destination"]

    assert {:ok, pfmerge} = Dispatcher.prepare_raw("PFMERGE", ["destination", "source"])
    assert pfmerge.read_keys == ["destination", "source"]
    assert pfmerge.write_keys == ["destination"]
  end

  test "read-modify-write commands and SET GET retain read access" do
    assert {:ok, plain_set} = Dispatcher.prepare_raw("SET", ["secret", "replacement"])
    assert plain_set.read_keys == []
    assert plain_set.write_keys == ["secret"]

    assert {:ok, set_get} =
             Dispatcher.prepare_raw("SET", ["secret", "replacement", "GET"])

    assert set_get.read_keys == ["secret"]
    assert set_get.write_keys == ["secret"]

    for {command, args} <- [
          {"INCR", ["counter"]},
          {"HSET", ["hash", "field", "value"]},
          {"ZADD", ["scores", "1", "member"]}
        ] do
      assert {:ok, prepared} = Dispatcher.prepare_raw(command, args)
      assert prepared.read_keys == prepared.acl_keys
      assert prepared.write_keys == prepared.acl_keys
    end
  end

  test "key access normalization accepts lowercase names and classifies pipelines" do
    assert KeyDiscovery.access_keys("copy", ["source", "destination"]) ==
             {["source"], ["destination"]}

    assert KeyDiscovery.command_access_type("pipeline") == :write
  end

  test "routing derives cross-shard status from prepared routing keys" do
    assert {:ok, prepared} = Dispatcher.prepare_raw("MGET", ["key:a", "key:b"])

    shard_for = fn
      "key:a" -> 0
      "key:b" -> 1
    end

    assert PreparedCommand.shard_indexes(prepared, shard_for) == [0, 1]
    assert PreparedCommand.cross_shard?(prepared, shard_for)
  end

  test "no-key commands do not invent ACL, routing, or mutation keys" do
    assert {:ok, prepared} = Dispatcher.prepare_raw("PING", [])

    assert prepared.acl_keys == []
    assert prepared.routing_keys == []
    assert prepared.read_keys == []
    assert prepared.write_keys == []

    assert PreparedCommand.shard_indexes(prepared, fn _key -> flunk("unexpected routing") end) ==
             []

    refute PreparedCommand.cross_shard?(prepared, fn _key -> flunk("unexpected routing") end)
  end

  test "a literal wildcard data key remains routable" do
    assert {:ok, prepared} = Dispatcher.prepare_raw("GET", ["*"])
    assert prepared.acl_keys == ["*"]
    assert prepared.routing_keys == ["*"]
    assert prepared.read_keys == ["*"]
  end

  test "Flow commands expose coordinated routing instead of logical ACL keys" do
    assert {:ok, policy_set} =
             Dispatcher.prepare_raw("FLOW.POLICY.SET", [
               "checkout",
               "INDEXED_ATTRIBUTES",
               "tenant"
             ])

    assert policy_set.acl_keys == ["checkout"]
    assert policy_set.routing_scope == :coordinated
    assert policy_set.routing_keys == []
    assert PreparedCommand.cross_shard?(policy_set, fn _key -> flunk("unexpected routing") end)

    assert {:ok, global_query} = Dispatcher.prepare_raw("FLOW.SEARCH", [])
    assert global_query.acl_keys == ["*"]
    assert global_query.routing_scope == :coordinated
    assert global_query.routing_keys == []
  end

  @tag :prepared_multi_routing
  test "global data mutations expose coordinated routing" do
    for command <- ["FLUSHDB", "FLUSHALL"] do
      assert {:ok, prepared} = Dispatcher.prepare_raw(command, [])
      assert prepared.routing_scope == :coordinated
      assert prepared.routing_keys == []
      assert PreparedCommand.cross_shard?(prepared, fn _key -> flunk("unexpected routing") end)
    end
  end

  test "global keyspace reads expose coordinated routing" do
    for {command, args} <- [
          {"DBSIZE", []},
          {"KEYS", ["*"]},
          {"RANDOMKEY", []},
          {"SCAN", ["0"]},
          {"FERRICSTORE.TELEMETRY", ["NAMESPACE_USAGE", "tenant"]}
        ] do
      assert {:ok, prepared} = Dispatcher.prepare_raw(command, args)
      assert prepared.routing_scope == :coordinated
      assert prepared.routing_keys == []
      assert PreparedCommand.cross_shard?(prepared, fn _key -> flunk("unexpected routing") end)
    end
  end

  test "tuple projection APIs are not part of the prepared-command contract" do
    refute function_exported?(Dispatcher, :parse_raw, 2)
    refute function_exported?(PreparedCommand, :legacy_result, 1)
  end

  test "legacy transaction queue entries derive their prepared AST" do
    assert {"HGETALL", ["hash"], {:hgetall, "hash"}} =
             TransactionAst.normalize_entry({"HGETALL", ["hash"]})
  end
end
