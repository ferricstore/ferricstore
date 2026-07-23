defmodule Ferricstore.Commands.NativeAstParserTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.{Extension, NativeAstParser}
  alias FerricstoreServer.Acl.CommandCategories

  @protocol_control_commands ~w(PIPELINE)
  @authorization_only_commands ~w(FLOW.QUERY.EXPLAIN)
  @not_implemented_redis_commands ~w(DUMP MIGRATE RESTORE SHUTDOWN SORT)
  @removed_flow_collection_commands ~w(
    FLOW.LIST FLOW.SEARCH FLOW.TERMINALS FLOW.FAILURES FLOW.STUCK
    FLOW.BY_PARENT FLOW.BY_ROOT FLOW.BY_CORRELATION
  )

  test "rejects nonconvertible command arguments without raising" do
    for invalid <- [%{}, {:tuple, "value"}, [0xD800]] do
      assert {:error, "ERR protocol error"} = NativeAstParser.parse("GET", [invalid])
    end
  end

  test "rejects float arguments outside the VM float range without raising" do
    huge = String.duplicate("9", 1_000)

    for {command, args} <- [
          {"INCRBYFLOAT", ["key", huge]},
          {"HINCRBYFLOAT", ["hash", "field", huge]},
          {"ZINCRBY", ["zset", huge, "member"]},
          {"ZADD", ["zset", huge, "member"]},
          {"BF.RESERVE", ["bloom", huge, "100"]},
          {"TDIGEST.TRIMMED_MEAN", ["digest", huge, "1"]},
          {"BLMPOP", [huge, "1", "list", "LEFT"]}
        ] do
      assert {:ok, ^command, _parsed_args, ast, _keys} = NativeAstParser.parse(command, args)
      assert ast_contains_error?(ast), "expected #{command} to contain a numeric parse error"
    end
  end

  test "native AST parser covers every ACL-visible command family" do
    supported = NativeAstParser.supported_command_names()

    unsupported =
      CommandCategories.categories()
      |> Map.fetch!("ALL")
      |> Enum.reject(&(&1 in @protocol_control_commands))
      |> Enum.reject(&(&1 in @authorization_only_commands))
      |> Enum.reject(&(&1 in @not_implemented_redis_commands))
      |> Enum.reject(&native_parser_covers?(&1, supported))
      |> Enum.sort()

    assert unsupported == []
  end

  test "parses every built-in ACL-visible command family without unknown AST fallback" do
    extension_names = Extension.command_names_upper()

    commands =
      CommandCategories.categories()
      |> Map.fetch!("ALL")
      |> Enum.reject(&(&1 in @protocol_control_commands))
      |> Enum.reject(&(&1 in @authorization_only_commands))
      |> Enum.reject(&(&1 in @not_implemented_redis_commands))
      |> Enum.reject(&MapSet.member?(extension_names, &1))

    for command <- commands do
      {name, args} = parser_call(command)
      assert {:ok, upper, _parsed_args, ast, keys} = NativeAstParser.parse(name, args)
      refute match?({:unknown, ^upper, _}, ast), "expected #{command} to be known"
      assert is_list(keys)
    end
  end

  test "collection scan cursors decode to seek boundaries" do
    for command <- ~w(HSCAN SSCAN ZSCAN) do
      tag = command |> String.downcase() |> String.to_atom()

      assert {:ok, ^command, _args, {^tag, "collection", {:after, "member-b"}, []},
              ["collection"]} =
               NativeAstParser.parse(command, ["collection", "~bWVtYmVyLWI"])

      assert {:ok, ^command, _args, {^tag, "collection", {:error, "ERR invalid cursor"}, []},
              ["collection"]} =
               NativeAstParser.parse(command, ["collection", "999999"])
    end
  end

  test "every built-in Flow data command has a non-empty ACL scope" do
    extension_names = Extension.command_names_upper()

    commands =
      CommandCategories.categories()
      |> Map.fetch!("ALL")
      |> Enum.filter(&String.starts_with?(&1, "FLOW."))
      |> Enum.reject(&(&1 in @authorization_only_commands))
      |> Enum.reject(&MapSet.member?(extension_names, &1))

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

  @tag :xreadgroup_acl_streams_operand
  test "XREADGROUP ACL extraction ignores STREAMS in group and consumer operands" do
    for {group, consumer} <- [{"STREAMS", "consumer"}, {"workers", "STREAMS"}] do
      assert {:ok, "XREADGROUP", _args, _ast, ["actual-stream"]} =
               NativeAstParser.parse("XREADGROUP", [
                 "GROUP",
                 group,
                 consumer,
                 "STREAMS",
                 "actual-stream",
                 ">"
               ])
    end
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

  test "generic Flow command families expose their effective ACL scopes" do
    for {command, args, expected_keys} <- [
          {"FLOW.STEP_CONTINUE", ["tenant:a:flow"], ["tenant:a:flow"]},
          {"FLOW.START_AND_CLAIM", ["tenant:a:flow"], ["tenant:a:flow"]},
          {"FLOW.SCHEDULE.GET", ["tenant:a:schedule"], ["*"]},
          {"FLOW.SCHEDULE.FIRE", ["tenant:a:schedule"], ["*"]},
          {"FLOW.SCHEDULE.PAUSE", ["tenant:a:schedule"], ["*"]},
          {"FLOW.SCHEDULE.RESUME", ["tenant:a:schedule"], ["*"]},
          {"FLOW.SCHEDULE.DELETE", ["tenant:a:schedule"], ["*"]},
          {"FLOW.APPROVAL.GET", ["tenant:a:approval"], ["*"]},
          {"FLOW.APPROVAL.APPROVE", ["tenant:a:approval", "admin"], ["*"]},
          {"FLOW.APPROVAL.REJECT", ["tenant:a:approval", "admin"], ["*"]},
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

  test "FLOW.QUERY keeps FQL version, query text, and named parameters distinct" do
    query =
      "FROM runs WHERE partition_key = @partition AND run_id = @flow_id RETURN RECORD"

    assert {:ok, "FLOW.QUERY", _args,
            {:flow_query, "FQL1", ^query, %{"partition" => "tenant-a", "flow_id" => "run-123"}},
            ["*"]} =
             NativeAstParser.parse("FLOW.QUERY", [
               "FQL1",
               query,
               "partition",
               "tenant-a",
               "flow_id",
               "run-123"
             ])

    assert {:ok, "FLOW.QUERY", _args, {:flow_query, {:error, error}}, ["*"]} =
             NativeAstParser.parse("FLOW.QUERY", ["FQL1", query, "dangling"])

    assert error =~ "parameter"

    params = Enum.flat_map(1..65, fn index -> ["parameter_#{index}", "value"] end)

    assert {:ok, "FLOW.QUERY", _args, {:flow_query, {:error, too_many_error}}, ["*"]} =
             NativeAstParser.parse("FLOW.QUERY", ["FQL1", query | params])

    assert too_many_error =~ "at most 64"
  end

  test "removed Flow collection commands have no parser route" do
    for command <- @removed_flow_collection_commands do
      assert {:ok, ^command, [], {:unknown, ^command, []}, []} =
               NativeAstParser.parse(command, [])
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

    assert {:ok, "HGETEX", _args, {:hgetex, "h", :none, ["f"]}, ["h"]} =
             NativeAstParser.parse("hgetex", ["h", "FIELDS", "1", "f"])

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

  test "TDigest quantile domains are validated while preparing commands" do
    for {command, args} <- [
          {"TDIGEST.QUANTILE", ["digest", "-0.1"]},
          {"TDIGEST.QUANTILE", ["digest", "1.1"]},
          {"TDIGEST.TRIMMED_MEAN", ["digest", "-0.1", "0.5"]},
          {"TDIGEST.TRIMMED_MEAN", ["digest", "0.5", "0.5"]},
          {"TDIGEST.TRIMMED_MEAN", ["digest", "0.7", "0.6"]}
        ] do
      assert {:ok, ^command, _parsed_args, ast, ["digest"]} =
               NativeAstParser.parse(command, args)

      assert ast_contains_error?(ast), "expected #{command} to contain a domain error"
    end
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

  @tag :prepared_unlink_keys
  test "counted and variadic commands expose every data key" do
    for {command, args, expected_keys} <- [
          {"SINTERCARD", ["2", "set:one", "set:two", "LIMIT", "1"], ["set:one", "set:two"]},
          {"PFCOUNT", ["hll:one", "hll:two"], ["hll:one", "hll:two"]},
          {"UNLINK", ["cache:one", "cache:two"], ["cache:one", "cache:two"]}
        ] do
      assert {:ok, ^command, _parsed_args, _ast, ^expected_keys} =
               NativeAstParser.parse(command, args)
    end
  end

  test "SINTERCARD validates its counted key list and LIMIT in the prepared parser" do
    assert {:ok, "SINTERCARD", _args, {:sintercard, ["set:one"], 1}, ["set:one"]} =
             NativeAstParser.parse("SINTERCARD", ["1", "set:one", "limit", "1"])

    for {args, reason} <- [
          {["0", "set:one"], "ERR numkeys can't be non-positive value"},
          {["2", "set:one"], "ERR Number of keys can't be greater than number of args"},
          {["invalid", "set:one"], "ERR numkeys can't be non-positive value"},
          {["1", "set:one", "LIMIT", "-1"], "ERR value is not an integer or out of range"},
          {["1", "set:one", "unexpected"], "ERR syntax error"}
        ] do
      assert {:ok, "SINTERCARD", _parsed_args, {:sintercard, {:error, ^reason}}, _keys} =
               NativeAstParser.parse("SINTERCARD", args)
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
          {"FETCH_OR_COMPUTE_RESULT", ["native:key", "token", "value", "100"]},
          {"FETCH_OR_COMPUTE_ERROR", ["native:key", "token", "error"]}
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

    assert {:ok, "FLOW.GET", _args, {:flow_get, "flow-1", []}, ["*"]} =
             NativeAstParser.parse("flow.get", ["flow-1"])

    assert {:ok, "FLOW.HISTORY", _args, {:flow_history, "flow-1", [partition_key: "tenant:a"]},
            ["tenant:a"]} =
             NativeAstParser.parse("flow.history", ["flow-1", "PARTITION", "tenant:a"])

    assert {:ok, "FLOW.HISTORY", _args, {:flow_history, "flow-1", []}, ["*"]} =
             NativeAstParser.parse("flow.history", ["flow-1"])

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

  test "TOPK.RESERVE omits the unsupported decay field" do
    assert {:ok, "TOPK.RESERVE", _args, {:topk_reserve, "tracker", 10, 20, 5}, ["tracker"]} =
             NativeAstParser.parse("topk.reserve", ["tracker", "10", "20", "5"])

    assert {:ok, "TOPK.RESERVE", _args, {:topk_reserve, {:error, message}}, ["tracker"]} =
             NativeAstParser.parse("topk.reserve", ["tracker", "10", "20", "5", "0.9"])

    assert message =~ "wrong number of arguments"
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

  test "parses Flow policy compare-and-swap and replacement controls through native AST" do
    assert {:ok, "FLOW.POLICY.SET", _args, {:flow_policy_set, "checkout", opts}, ["checkout"]} =
             NativeAstParser.parse("flow.policy.set", [
               "checkout",
               "EXPECTED_GENERATION",
               "7",
               "REPLACE",
               "TRUE"
             ])

    assert opts[:expected_generation] == 7
    assert opts[:replace] == true
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

  defp ast_contains_error?({:error, message}) when is_binary(message), do: true

  defp ast_contains_error?(tuple) when is_tuple(tuple) do
    tuple
    |> Tuple.to_list()
    |> Enum.any?(&ast_contains_error?/1)
  end

  defp ast_contains_error?(list) when is_list(list), do: Enum.any?(list, &ast_contains_error?/1)
  defp ast_contains_error?(_term), do: false

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
