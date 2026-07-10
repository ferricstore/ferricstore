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

  test "every Flow command has a non-empty ACL scope" do
    commands =
      CommandCategories.categories()
      |> Map.fetch!("ALL")
      |> Enum.filter(&String.starts_with?(&1, "FLOW."))

    for command <- commands do
      {name, args} = parser_call(command)
      assert {:ok, _upper, _parsed_args, _ast, keys} = NativeAstParser.parse(name, args)
      refute keys == [], "expected #{command} to require an ACL scope"
    end
  end

  test "Flow ACL extraction uses parsed options rather than matching option values" do
    assert {:ok, "FLOW.CLAIM_DUE", _args,
            {:flow_claim_due, "tenant:a:type", [worker: "PARTITION", lease_ms: 30_000]}, ["*"]} =
             NativeAstParser.parse("FLOW.CLAIM_DUE", [
               "tenant:a:type",
               "WORKER",
               "PARTITION",
               "LEASE_MS",
               "30000"
             ])
  end

  test "Flow batch ACL extraction preserves mixed item partitions" do
    assert {:ok, "FLOW.CANCEL_MANY", _args,
            {:flow_cancel_many, nil,
             [{:id, "tenant:a:flow", :partition_key, "tenant:b:partition", :fencing_token, 7}],
             []}, ["tenant:b:partition"]} =
             NativeAstParser.parse("FLOW.CANCEL_MANY", [
               "MIXED",
               "ITEMS",
               "tenant:a:flow",
               "tenant:b:partition",
               "7"
             ])
  end

  test "generic Flow command families expose their unambiguous ACL scopes" do
    for {command, args, expected_keys} <- [
          {"FLOW.STEP_CONTINUE", ["tenant:a:flow"], ["tenant:a:flow"]},
          {"FLOW.START_AND_CLAIM", ["tenant:a:flow"], ["tenant:a:flow"]},
          {"FLOW.SCHEDULE.GET", ["tenant:a:schedule"], ["tenant:a:schedule"]},
          {"FLOW.APPROVAL.GET", ["tenant:a:approval"], ["tenant:a:approval"]},
          {"FLOW.EFFECT.GET", ["tenant:a:flow"], ["tenant:a:flow"]},
          {"FLOW.GOVERNANCE.LEDGER", ["tenant:a:flow"], ["tenant:a:flow"]},
          {"FLOW.CIRCUIT.OPEN", ["tenant:a:scope"], ["tenant:a:scope"]},
          {"FLOW.BUDGET.COMMIT", ["tenant:a:scope"], ["tenant:a:scope"]},
          {"FLOW.LIMIT.SPEND", ["tenant:a:scope"], ["tenant:a:scope"]},
          {"FLOW.SCHEDULE.FIRE_DUE", [], ["*"]},
          {"FLOW.APPROVAL.LIST", [], ["*"]},
          {"FLOW.GOVERNANCE.OVERVIEW", [], ["*"]}
        ] do
      assert {:ok, ^command, _parsed_args, _ast, ^expected_keys} =
               NativeAstParser.parse(command, args)
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

  test "merge commands expose destination and every source key" do
    assert {:ok, "CMS.MERGE", _args, {:cms_merge, "cms:dest", ["cms:one", "cms:two"], [2, 3]},
            ["cms:dest", "cms:one", "cms:two"]} =
             NativeAstParser.parse("CMS.MERGE", [
               "cms:dest",
               "2",
               "cms:one",
               "cms:two",
               "WEIGHTS",
               "2",
               "3"
             ])

    assert {:ok, "TDIGEST.MERGE", _args,
            {:tdigest_merge, "tdigest:dest", ["tdigest:one", "tdigest:two"], [override: true]},
            ["tdigest:dest", "tdigest:one", "tdigest:two"]} =
             NativeAstParser.parse("TDIGEST.MERGE", [
               "tdigest:dest",
               "2",
               "tdigest:one",
               "tdigest:two",
               "OVERRIDE"
             ])
  end

  test "counted and variadic commands expose every data key" do
    for {command, args, expected_keys} <- [
          {"SINTERCARD", ["2", "set:one", "set:two", "LIMIT", "1"], ["set:one", "set:two"]},
          {"PFCOUNT", ["hll:one", "hll:two"], ["hll:one", "hll:two"]}
        ] do
      assert {:ok, ^command, _parsed_args, _ast, ^expected_keys} =
               NativeAstParser.parse(command, args)
    end
  end

  test "subcommand operations expose the data key instead of the subcommand" do
    for {command, args, expected_keys} <- [
          {"OBJECT", ["ENCODING", "object:key"], ["object:key"]},
          {"MEMORY", ["USAGE", "memory:key"], ["memory:key"]},
          {"XINFO", ["STREAM", "stream:key"], ["stream:key"]},
          {"XGROUP", ["CREATE", "stream:key", "group-1", "$", "MKSTREAM"], ["stream:key"]}
        ] do
      assert {:ok, ^command, _parsed_args, _ast, ^expected_keys} =
               NativeAstParser.parse(command, args)
    end
  end

  test "extra native operations expose their first key" do
    for {command, args} <- [
          {"CAS", ["native:key", "expected", "replacement"]},
          {"LOCK", ["native:key", "owner", "100"]},
          {"UNLOCK", ["native:key", "owner"]},
          {"EXTEND", ["native:key", "owner", "100"]},
          {"RATELIMIT.ADD", ["native:key", "100", "10"]},
          {"KEY_INFO", ["native:key"]},
          {"FERRICSTORE.KEY_INFO", ["native:key"]},
          {"FETCH_OR_COMPUTE", ["native:key", "100", "hint"]},
          {"FETCH_OR_COMPUTE_RESULT", ["native:key", "value", "100"]},
          {"FETCH_OR_COMPUTE_ERROR", ["native:key", "error"]}
        ] do
      assert {:ok, ^command, _parsed_args, _ast, ["native:key"]} =
               NativeAstParser.parse(command, args)
    end
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
               "MAX_ACTIVE_MS",
               "30000",
               "PARTITION",
               "tenant-a",
               "ATTRIBUTE",
               "tenant",
               "acme"
             ])

    assert create_opts[:type] == "checkout"
    assert create_opts[:state] == "queued"
    assert create_opts[:payload] == "payload"
    assert create_opts[:max_active_ms] == 30_000
    assert create_opts[:attributes] == %{"tenant" => "acme"}

    assert {:ok, "FLOW.CREATE", _args, {:flow_create, "flow-infinity", create_opts},
            ["flow-infinity"]} =
             NativeAstParser.parse("flow.create", [
               "flow-infinity",
               "TYPE",
               "checkout",
               "MAX_ACTIVE_MS",
               "INFINITY"
             ])

    assert create_opts[:max_active_ms] == :infinity

    assert {:ok, "FLOW.CLAIM_DUE", _args, {:flow_claim_due, "checkout", claim_opts}, ["*"]} =
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

  test "parses Flow policy max active timeout through native AST" do
    assert {:ok, "FLOW.POLICY.SET", _args, {:flow_policy_set, "checkout", opts}, ["checkout"]} =
             NativeAstParser.parse("flow.policy.set", [
               "checkout",
               "MAX_ACTIVE_MS",
               "30000"
             ])

    assert opts[:max_active_ms] == 30_000

    assert {:ok, "FLOW.POLICY.SET", _args, {:flow_policy_set, "checkout", opts}, ["checkout"]} =
             NativeAstParser.parse("flow.policy.set", [
               "checkout",
               "MAX_ACTIVE_MS",
               "INFINITY"
             ])

    assert opts[:max_active_ms] == :infinity
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

    assert {:ok, "FLOW.POLICY.SET", _args,
            {:flow_policy_set, "checkout", {:error, "ERR flow max_active_ms is type-level only"}},
            ["checkout"]} =
             NativeAstParser.parse("flow.policy.set", [
               "checkout",
               "STATE",
               "queued",
               "MAX_ACTIVE_MS",
               "30000"
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
