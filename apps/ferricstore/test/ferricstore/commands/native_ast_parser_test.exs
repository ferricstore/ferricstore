defmodule Ferricstore.Commands.NativeAstParserTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.NativeAstParser
  alias FerricstoreServer.Acl.CommandCategories

  @protocol_control_commands ~w(PIPELINE)
  @not_implemented_redis_commands ~w(DUMP MIGRATE RESTORE SHUTDOWN SORT)

  test "native AST parser covers every ACL-visible command family" do
    supported = NativeAstParser.supported_command_names()

    unsupported =
      CommandCategories.categories()
      |> Map.fetch!("ALL")
      |> Enum.reject(&(&1 in @protocol_control_commands))
      |> Enum.reject(&(&1 in @not_implemented_redis_commands))
      |> Enum.reject(&native_parser_covers?(&1, supported))
      |> Enum.sort()

    assert unsupported == []
  end

  test "parses every ACL-visible command family without unknown AST fallback" do
    commands =
      CommandCategories.categories()
      |> Map.fetch!("ALL")
      |> Enum.reject(&(&1 in @protocol_control_commands))
      |> Enum.reject(&(&1 in @not_implemented_redis_commands))

    for command <- commands do
      {name, args} = parser_call(command)
      assert {:ok, upper, _parsed_args, ast, keys} = NativeAstParser.parse(name, args)
      refute match?({:unknown, ^upper, _}, ast), "expected #{command} to be known"
      assert is_list(keys)
    end
  end

  test "parses hot string command ASTs directly" do
    assert {:ok, "GET", ["k"], {:get, "k"}, ["k"]} = NativeAstParser.parse("get", ["k"])

    assert {:ok, "SET", ["k", "v", "nx", "ex", "5"], {:set, "k", "v", [:nx, {:ex, 5}]}, ["k"]} =
             NativeAstParser.parse("set", ["k", "v", "nx", "ex", "5"])

    assert {:ok, "SET", _args,
            {:set, "k", "v",
             {:error, "ERR XX and NX options at the same time are not compatible"}}, ["k"]} =
             NativeAstParser.parse("set", ["k", "v", "NX", "XX"])
  end

  test "parses collection and probabilistic command families" do
    assert {:ok, "HEXPIRE", _args, {:hexpire, "h", 10, ["f"]}, ["h"]} =
             NativeAstParser.parse("hexpire", ["h", "10", "FIELDS", "1", "f"])

    assert {:ok, "HPEXPIRE", _args, {:hpexpire, "h", 250, ["f"]}, ["h"]} =
             NativeAstParser.parse("hpexpire", ["h", "250", "FIELDS", "1", "f"])

    assert {:ok, "HTTL", _args, {:httl, "h", ["f"]}, ["h"]} =
             NativeAstParser.parse("httl", ["h", "FIELDS", "1", "f"])

    assert {:ok, "HPERSIST", _args, {:hpersist, "h", ["f"]}, ["h"]} =
             NativeAstParser.parse("hpersist", ["h", "FIELDS", "1", "f"])

    assert {:ok, "HGETEX", _args, {:hgetex, "h", :persist, ["f"]}, ["h"]} =
             NativeAstParser.parse("hgetex", ["h", "PERSIST", "FIELDS", "1", "f"])

    assert {:ok, "HSETEX", _args, {:hsetex, "h", 5, ["f", "v"]}, ["h"]} =
             NativeAstParser.parse("hsetex", ["h", "5", "f", "v"])

    assert {:ok, "ZRANGE", ["z", "0", "-1"], {:zrange, "z", 0, -1, false}, ["z"]} =
             NativeAstParser.parse("zrange", ["z", "0", "-1"])

    assert {:ok, "ZRANDMEMBER", _args, {:zrandmember, "z", 1, false}, ["z"]} =
             NativeAstParser.parse("zrandmember", ["z", "1"])

    assert {:ok, "ZRANDMEMBER", _args, {:zrandmember, "z", 1, true}, ["z"]} =
             NativeAstParser.parse("zrandmember", ["z", "1", "WITHSCORES"])

    assert {:ok, "XADD", ["events", "*", "field", "value"],
            {:xadd, ["events", "*", "field", "value"]}, ["events"]} =
             NativeAstParser.parse("xadd", ["events", "*", "field", "value"])

    assert {:ok, "GEOADD", ["places", "1", "2", "home"], {:geoadd, ["places", "1", "2", "home"]},
            ["places"]} =
             NativeAstParser.parse("geoadd", ["places", "1", "2", "home"])

    assert {:ok, "BF.RESERVE", _args, {:bf_reserve, "bf", 0.01, 1000}, ["bf"]} =
             NativeAstParser.parse("bf.reserve", ["bf", "0.01", "1000"])
  end

  test "parses Flow create claim terminal and query ASTs" do
    assert {:ok, "FLOW.CREATE", _args, {:flow_create, "flow-1", create_opts}, ["tenant-a"]} =
             NativeAstParser.parse("flow.create", [
               "flow-1",
               "TYPE",
               "checkout",
               "STATE",
               "queued",
               "PAYLOAD",
               "payload",
               "RUN_AT",
               "1000",
               "PRIORITY",
               "2",
               "PARTITION",
               "tenant-a",
               "ATTRIBUTE",
               "tenant",
               "acme"
             ])

    assert create_opts[:type] == "checkout"
    assert create_opts[:state] == "queued"
    assert create_opts[:payload] == "payload"
    assert create_opts[:attributes] == %{"tenant" => "acme"}

    assert {:ok, "FLOW.CLAIM_DUE", _args, {:flow_claim_due, "checkout", claim_opts}, ["checkout"]} =
             NativeAstParser.parse("flow.claim_due", [
               "checkout",
               "WORKER",
               "worker-a",
               "LEASE_MS",
               "30000",
               "LIMIT",
               "100",
               "PAYLOAD",
               "ATTRIBUTE",
               "tenant",
               "acme"
             ])

    assert claim_opts[:worker] == "worker-a"
    assert claim_opts[:lease_ms] == 30_000
    assert claim_opts[:limit] == 100
    assert claim_opts[:payload] == true
    assert claim_opts[:attributes] == %{"tenant" => "acme"}

    assert {:ok, "FLOW.COMPLETE", _args,
            {:flow_complete, "flow-1", "lease-1", [fencing_token: 1, result: "ok"]}, ["flow-1"]} =
             NativeAstParser.parse("flow.complete", [
               "flow-1",
               "lease-1",
               "FENCING",
               "1",
               "RESULT",
               "ok"
             ])

    assert {:ok, "FLOW.ATTRIBUTE_VALUES", _args,
            {:flow_attribute_values, "checkout", "tenant",
             [count: 10, partition_key: "tenant-a"]}, ["tenant-a"]} =
             NativeAstParser.parse("flow.attribute_values", [
               "checkout",
               "tenant",
               "COUNT",
               "10",
               "PARTITION",
               "tenant-a"
             ])
  end

  test "extracts namespace ACL keys from direct Flow metadata commands" do
    assert {:ok, "FLOW.GET", _args, {:flow_get, "flow-1", [partition_key: "tenant:a"]},
            ["tenant:a"]} =
             NativeAstParser.parse("flow.get", ["flow-1", "PARTITION", "tenant:a"])

    assert {:ok, "FLOW.GET", _args, {:flow_get, "flow-1", []}, ["flow-1"]} =
             NativeAstParser.parse("flow.get", ["flow-1"])

    assert {:ok, "FLOW.HISTORY", _args, {:flow_history, "flow-1", [partition_key: "tenant:a"]},
            ["tenant:a"]} =
             NativeAstParser.parse("flow.history", ["flow-1", "PARTITION", "tenant:a"])

    assert {:ok, "FLOW.LIST", _args, {:flow_list, "checkout", [partition_key: "tenant:a"]},
            ["tenant:a"]} =
             NativeAstParser.parse("flow.list", ["checkout", "PARTITION", "tenant:a"])

    assert {:ok, "FLOW.SEARCH", _args, {:flow_search, search_opts}, ["tenant:a"]} =
             NativeAstParser.parse("flow.search", [
               "TYPE",
               "checkout",
               "ATTRIBUTE",
               "tenant",
               "acme",
               "COUNT",
               "10",
               "PARTITION",
               "tenant:a"
             ])

    assert search_opts[:type] == "checkout"
    assert search_opts[:attributes] == %{"tenant" => "acme"}
    assert search_opts[:count] == 10
    assert search_opts[:partition_key] == "tenant:a"

    assert {:ok, "FLOW.SEARCH", _args, {:flow_search, search_opts}, ["*"]} =
             NativeAstParser.parse("flow.search", [
               "TYPE",
               "checkout",
               "STATE_META",
               "running",
               "step",
               "charge"
             ])

    assert search_opts[:type] == "checkout"
    assert search_opts[:state_meta] == %{"running" => %{"step" => "charge"}}

    assert {:ok, "FLOW.ATTRIBUTES", _args,
            {:flow_attributes, "checkout", [partition_key: "tenant:a"]}, ["tenant:a"]} =
             NativeAstParser.parse("flow.attributes", ["checkout", "PARTITION", "tenant:a"])

    assert {:ok, "FLOW.ATTRIBUTE_VALUES", _args,
            {:flow_attribute_values, "checkout", "tenant", [partition_key: "tenant:a"]},
            ["tenant:a"]} =
             NativeAstParser.parse("flow.attribute_values", [
               "checkout",
               "tenant",
               "PARTITION",
               "tenant:a"
             ])

    assert {:ok, "FLOW.ATTRIBUTES", _args, {:flow_attributes, "checkout", []}, ["*"]} =
             NativeAstParser.parse("flow.attributes", ["checkout"])

    assert {:ok, "FLOW.ATTRIBUTE_VALUES", _args,
            {:flow_attribute_values, "checkout", "tenant", []}, ["*"]} =
             NativeAstParser.parse("flow.attribute_values", ["checkout", "tenant"])
  end

  test "parses Flow policy indexed attributes through native AST" do
    assert {:ok, "FLOW.POLICY.SET", _args, {:flow_policy_set, "checkout", opts}, ["checkout"]} =
             NativeAstParser.parse("flow.policy.set", [
               "checkout",
               "INDEXED_ATTRIBUTES",
               "tenant,region",
               "INDEXED_STATE_META",
               "version"
             ])

    assert opts[:indexed_attributes] == ["tenant", "region"]
    assert opts[:indexed_state_meta] == "version"

    assert {:ok, "FLOW.POLICY.SET", _args,
            {:flow_policy_set, "checkout", [indexed_attributes: ["tenant"]]}, ["checkout"]} =
             NativeAstParser.parse("flow.policy.set", [
               "checkout",
               "INDEXED_ATTRIBUTES",
               "tenant"
             ])

    assert {:ok, "FLOW.POLICY.SET", _args,
            {:flow_policy_set, "checkout", [indexed_attributes: ["tenant", "region"]]},
            ["checkout"]} =
             NativeAstParser.parse("flow.policy.set", [
               "checkout",
               "INDEXED_ATTRIBUTES",
               ~s(["tenant","region"])
             ])
  end

  test "rejects state-level Flow policy indexed attributes through native AST" do
    assert {:ok, "FLOW.POLICY.SET", _args,
            {:flow_policy_set, "checkout",
             {:error, "ERR flow indexed_attributes is type-level only"}}, ["checkout"]} =
             NativeAstParser.parse("flow.policy.set", [
               "checkout",
               "STATE",
               "queued",
               "INDEXED_ATTRIBUTES",
               "tenant"
             ])
  end

  test "parses Flow many and newer workflow/governance command names" do
    assert {:ok, "FLOW.CREATE_MANY", _args,
            {:flow_create_many, "tenant-a",
             [{:id, "flow-1", :payload, "p1"}, {:id, "flow-2", :payload, "p2"}],
             [type: "checkout", state: "queued", independent: true]}, _keys} =
             NativeAstParser.parse("flow.create_many", [
               "tenant-a",
               "TYPE",
               "checkout",
               "STATE",
               "queued",
               "INDEPENDENT",
               "true",
               "ITEMS",
               "flow-1",
               "p1",
               "flow-2",
               "p2"
             ])

    for command <- [
          "flow.schedule.create",
          "flow.effect.reserve",
          "flow.approval.request",
          "flow.circuit.open",
          "flow.budget.reserve",
          "flow.limit.lease",
          "flow.run_steps_many"
        ] do
      assert {:ok, upper, _args, ast, _keys} = NativeAstParser.parse(command, [])
      refute match?({:unknown, ^upper, _}, ast)
    end
  end

  defp native_parser_covers?(command, supported) do
    MapSet.member?(supported, command) or command_parent_supported?(command, supported)
  end

  defp command_parent_supported?(command, supported) do
    case String.split(command, ".", parts: 2) do
      [parent, _subcommand]
      when parent in ["ACL", "CLIENT", "CLUSTER", "CONFIG", "FERRICSTORE"] ->
        MapSet.member?(supported, parent)

      _ ->
        false
    end
  end

  defp parser_call("FLOW.SPAWN_CHILDREN"),
    do: {"FLOW.SPAWN_CHILDREN", ["parent", "ITEMS", "child", "child-type", "payload"]}

  defp parser_call("ACL." <> subcommand), do: {"ACL", [subcommand]}
  defp parser_call("CLIENT." <> subcommand), do: {"CLIENT", [subcommand]}
  defp parser_call(command), do: {command, []}
end
