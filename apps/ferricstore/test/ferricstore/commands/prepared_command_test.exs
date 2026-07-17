defmodule Ferricstore.Commands.PreparedCommandTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Dispatcher, KeyDiscovery, PreparedCommand}
  alias Ferricstore.Transaction.ExecutionEntry
  alias Ferricstore.Transaction.Ast, as: TransactionAst

  test "unknown commands are rejected before ACL and routing metadata are constructed" do
    assert {:error, "ERR unknown command 'brpoplpush', with args beginning with: "} =
             Dispatcher.prepare_raw("BRPOPLPUSH", ["source", "destination", "1"])

    assert {:error, "ERR unknown command 'not.a.command', with args beginning with: "} =
             Dispatcher.prepare_raw("NOT.A.COMMAND", ["secret:key"])
  end

  @tag :shared_command_spec
  test "KeyDiscovery prepares parsing, ACL, routing, and mutation metadata together" do
    assert {:ok, spec} = KeyDiscovery.prepare("set", ["key", "value", "GET"])

    assert spec.command == "SET"
    assert spec.args == ["key", "value", "GET"]
    assert spec.ast == {:set, "key", "value", [:get]}
    assert spec.command_keys == ["key"]
    assert spec.acl_keys == ["key"]
    assert spec.routing_scope == :keys
    assert spec.routing_keys == ["key"]
    assert spec.read_keys == ["key"]
    assert spec.write_keys == ["key"]
    assert spec.transaction_mode == :local
  end

  test "Flow preparation separates COMMAND GETKEYS data keys from conservative ACL scope" do
    assert {:ok, unpartitioned} = KeyDiscovery.prepare("FLOW.GET", ["flow-1"])
    assert unpartitioned.command_keys == ["flow-1"]
    assert unpartitioned.acl_keys == ["*"]

    assert {:ok, partitioned} =
             KeyDiscovery.prepare("FLOW.GET", ["flow-1", "PARTITION", "tenant-a"])

    assert partitioned.command_keys == ["tenant-a"]
    assert partitioned.acl_keys == ["tenant-a"]
  end

  test "KeyDiscovery describe keeps the command key contract" do
    description = KeyDiscovery.describe("FLOW.GET", {:flow_get, "flow-1", []}, ["*"])

    assert description.command_keys == ["flow-1"]
    assert description.acl_keys == ["*"]
  end

  @tag :shared_command_spec
  test "PreparedCommand has one canonical preparation boundary" do
    source =
      Path.expand("../../../lib/ferricstore/commands/prepared_command.ex", __DIR__)
      |> File.read!()

    assert source =~ "KeyDiscovery.prepare"
    refute source =~ "NativeAstParser"
    refute source =~ "Extension"
    refute source =~ "def from_parsed"
    refute function_exported?(PreparedCommand, :from_parsed, 4)
  end

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

  test "Pub/Sub preparation separates channel ACLs from data-key ACLs" do
    assert {:ok, prepared} = Dispatcher.prepare_raw("PUBLISH", ["tenant:events", "payload"])

    assert prepared.command_keys == ["tenant:events"]
    assert prepared.channel_keys == ["tenant:events"]
    assert prepared.acl_keys == []
    assert prepared.routing_scope == :none
    assert prepared.routing_keys == []
    assert prepared.read_keys == []
    assert prepared.write_keys == []
  end

  test "prepared commands identify deterministic transaction-local execution" do
    for {command, args} <- [
          {"PING", []},
          {"SET", ["key", "value"]},
          {"MEMORY", ["USAGE", "key"]},
          {"CLUSTER.KEYSLOT", ["key"]}
        ] do
      assert {:ok, prepared} = Dispatcher.prepare_raw(command, args)
      assert prepared.transaction_mode == :local
      assert PreparedCommand.transaction_safe?(prepared)
    end
  end

  @tag :transaction_namespace_contract
  test "transaction-local multi-key ASTs namespace every prepared routing key" do
    namespace = "tenant:"

    fixtures = [
      {"DEL", ["del:a", "del:b"]},
      {"UNLINK", ["unlink:a", "unlink:b"]},
      {"EXISTS", ["exists:a", "exists:b"]},
      {"MGET", ["mget:a", "mget:b"]},
      {"MSET", ["mset:a", "value-a", "mset:b", "value-b"]},
      {"MSETNX", ["msetnx:a", "value-a", "msetnx:b", "value-b"]},
      {"SINTER", ["sinter:a", "sinter:b"]},
      {"SUNION", ["sunion:a", "sunion:b"]},
      {"SDIFF", ["sdiff:a", "sdiff:b"]},
      {"SDIFFSTORE", ["sdiffstore:dest", "sdiffstore:a", "sdiffstore:b"]},
      {"SINTERSTORE", ["sinterstore:dest", "sinterstore:a", "sinterstore:b"]},
      {"SUNIONSTORE", ["sunionstore:dest", "sunionstore:a", "sunionstore:b"]},
      {"SINTERCARD", ["2", "sintercard:a", "sintercard:b"]},
      {"PFCOUNT", ["pfcount:a", "pfcount:b"]},
      {"PFMERGE", ["pfmerge:dest", "pfmerge:a", "pfmerge:b"]},
      {"BITOP", ["AND", "bitop:dest", "bitop:a", "bitop:b"]},
      {"COPY", ["copy:source", "copy:destination"]},
      {"RENAME", ["rename:source", "rename:destination"]},
      {"RENAMENX", ["renamenx:source", "renamenx:destination"]},
      {"LMOVE", ["lmove:source", "lmove:destination", "LEFT", "RIGHT"]},
      {"RPOPLPUSH", ["rpoplpush:source", "rpoplpush:destination"]},
      {"SMOVE", ["smove:source", "smove:destination", "member"]},
      {"GEOSEARCHSTORE",
       [
         "geosearchstore:destination",
         "geosearchstore:source",
         "FROMLONLAT",
         "0",
         "0",
         "BYRADIUS",
         "1",
         "KM"
       ]}
    ]

    for {command, args} <- fixtures do
      assert {:ok, prepared} = Dispatcher.prepare_raw(command, args)
      assert prepared.transaction_mode == :local
      assert length(prepared.routing_keys) > 1

      ast_binaries =
        prepared.ast
        |> TransactionAst.namespace_ast_keys(namespace)
        |> collect_ast_binaries()

      for key <- prepared.routing_keys do
        assert (namespace <> key) in ast_binaries,
               "#{command} did not namespace routing key #{inspect(key)}"

        refute key in ast_binaries,
               "#{command} retained unnamespaced routing key #{inspect(key)}"
      end
    end
  end

  @tag :transaction_stream_cache_commit
  test "prepared commands fail closed when replicated apply would escape the local store" do
    for {command, args} <- [
          {"PUBLISH", ["channel", "message"]},
          {"KEY_INFO", ["key"]},
          {"FETCH_OR_COMPUTE", ["key", "1000"]},
          {"XADD", ["stream", "1-0", "field", "value"]},
          {"XLEN", ["stream"]},
          {"XREAD", ["BLOCK", "1000", "STREAMS", "stream", "0-0"]},
          {"XACK", ["stream", "group", "1-0"]},
          {"SPOP", ["set"]},
          {"BF.ADD", ["filter", "member"]},
          {"FLOW.GET", ["flow-id"]}
        ] do
      assert {:ok, prepared} = Dispatcher.prepare_raw(command, args)
      assert prepared.transaction_mode == :request
      refute PreparedCommand.transaction_safe?(prepared)
    end
  end

  @tag :transaction_native_local_apply
  test "native mutations remain request-scoped until their apply contract is admitted" do
    for {command, args} <- [
          {"CAS", ["native:key", "old", "new"]},
          {"LOCK", ["native:key", "owner", "1000"]},
          {"UNLOCK", ["native:key", "owner"]},
          {"EXTEND", ["native:key", "owner", "1000"]},
          {"RATELIMIT.ADD", ["native:key", "1000", "10", "1"]}
        ] do
      assert {:ok, prepared} = Dispatcher.prepare_raw(command, args)
      assert prepared.transaction_mode == :request
      refute PreparedCommand.transaction_safe?(prepared)
    end
  end

  test "transaction execution entries retain only the compact validated apply plan" do
    value = :binary.copy("v", 4_096)
    assert {:ok, prepared} = Dispatcher.prepare_raw("SET", ["key", value])
    assert {:ok, entry} = ExecutionEntry.from_prepared(prepared)

    assert %ExecutionEntry{
             command: "SET",
             ast: {:set, "key", ^value},
             routing_scope: :keys
           } = entry

    assert entry.read_keys == []
    assert entry.write_keys == ["key"]
    assert ExecutionEntry.mutates?(entry)
    refute entry |> Map.put(:write_keys, []) |> ExecutionEntry.valid?()

    refute entry
           |> Map.merge(%{
             ast: {:set, "victim", value},
             command_keys: ["anchor"],
             write_keys: ["anchor"]
           })
           |> ExecutionEntry.valid?()

    assert {:ok, get_prepared} = Dispatcher.prepare_raw("GET", ["key"])
    assert {:ok, get_entry} = ExecutionEntry.from_prepared(get_prepared)
    refute ExecutionEntry.mutates?(get_entry)

    refute Map.has_key?(Map.from_struct(entry), :args)

    assert :erlang.external_size(entry) <
             :erlang.external_size({prepared.command, prepared.args, prepared.ast})

    assert {:ok, unsafe} = Dispatcher.prepare_raw("PUBLISH", ["channel", "message"])
    assert {:error, _reason} = ExecutionEntry.from_prepared(unsafe)
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

  test "raw transaction queue tuples are not part of the execution contract" do
    assert_raise FunctionClauseError, fn ->
      apply(TransactionAst, :normalize_entry, [{"HGETALL", ["hash"]}])
    end
  end

  defp collect_ast_binaries(value) when is_binary(value), do: [value]

  defp collect_ast_binaries(value) when is_list(value),
    do: Enum.flat_map(value, &collect_ast_binaries/1)

  defp collect_ast_binaries(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> collect_ast_binaries()
  end

  defp collect_ast_binaries(_value), do: []
end
