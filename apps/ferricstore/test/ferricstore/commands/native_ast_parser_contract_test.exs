defmodule Ferricstore.Commands.NativeAstParserContractTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.NativeAstParser

  test "prepared SCAN families produce typed, bounded options" do
    assert ast("SCAN", ["0", "match", "user:*", "count", "25", "type", "hash"]) ==
             {:scan, "0", [match: "user:*", count: 25, type: "hash"]}

    for command <- ~w(HSCAN SSCAN ZSCAN) do
      tag = command |> String.downcase() |> String.to_atom()

      assert ast(command, ["collection", "0", "match", "field:*", "count", "25"]) ==
               {tag, "collection", 0, [match: "field:*", count: 25]}

      assert ast(command, ["collection", "0", "COUNT", "10001"]) ==
               {tag, "collection", 0,
                {:error, "ERR value is not an integer or out of range"}}

      assert ast(command, ["collection", "0", "TYPE", "hash"]) ==
               {tag, "collection", 0, {:error, "ERR syntax error"}}
    end

    assert ast("SCAN", ["0", "COUNT", "10001"]) ==
             {:scan, "0", {:error, "ERR value is not an integer or out of range"}}
  end

  test "prepared COPY rejects every unsupported or duplicate option" do
    assert ast("COPY", ["source", "destination"]) ==
             {:copy, "source", "destination", false}

    assert ast("COPY", ["source", "destination", "replace"]) ==
             {:copy, "source", "destination", true}

    for opts <- [["unexpected"], ["REPLACE", "REPLACE"], ["REPLACE", "unexpected"]] do
      assert ast("COPY", ["source", "destination" | opts]) ==
               {:copy, "source", "destination", {:error, "ERR syntax error"}}
    end
  end

  test "prepared command keywords are case-insensitive" do
    assert ast("HEXPIRE", ["hash", "10", "fields", "1", "field"]) ==
             {:hexpire, "hash", 10, ["field"]}

    assert ast("HGETEX", ["hash", "persist", "fields", "1", "field"]) ==
             {:hgetex, "hash", :persist, ["field"]}

    assert ast("HGETEX", ["hash", "px", "10", "fields", "1", "field"]) ==
             {:hgetex, "hash", {:px, 10}, ["field"]}

    assert ast("BLMPOP", ["1", "1", "list", "left", "count", "2"]) ==
             {:blmpop, ["list"], :left, 2, 1_000}

    assert ast("TOPK.LIST", ["top", "withcount"]) == {:topk_list, "top", true}

    assert ast("TDIGEST.CREATE", ["digest", "compression", "100"]) ==
             {:tdigest_create, "digest", 100}

    assert ast("CMS.MERGE", ["dst", "1", "src", "weights", "2"]) ==
             {:cms_merge, "dst", ["src"], [2]}

    assert ast("TDIGEST.MERGE", ["dst", "1", "src", "override", "compression", "100"]) ==
             {:tdigest_merge, "dst", ["src"], [override: true, compression: 100]}

    assert ast("OBJECT", ["help"]) == {:object, :help}
  end

  test "lowercase XREADGROUP grammar still exposes every stream ACL key" do
    assert {:ok, "XREADGROUP", _args, _ast, ["stream:one", "stream:two"]} =
             NativeAstParser.parse("XREADGROUP", [
               "group",
               "workers",
               "consumer-1",
               "streams",
               "stream:one",
               "stream:two",
               ">",
               ">"
             ])
  end

  test "prepared integer and blocking-timeout values are bounded" do
    huge_integer = Integer.to_string(9_223_372_036_854_775_808)

    assert ast("WAIT", [huge_integer, "1"]) ==
             {:wait, {:error, "ERR value is not an integer or out of range"}, 1}

    assert ast("FLOW.CREATE", ["flow", "NOW", huge_integer]) ==
             {:flow_create, "flow", {:error, "ERR value is not an integer or out of range"}}

    assert ast("BLMPOP", ["1", "0", "left"]) ==
             {:blmpop, {:error, "ERR value is not an integer or out of range"}}

    for {command, args} <- [
          {"BLPOP", ["list", "4294968"]},
          {"BLMOVE", ["source", "destination", "LEFT", "RIGHT", "4294968"]},
          {"BLMPOP", ["4294968", "1", "list", "LEFT"]}
        ] do
      assert ast(command, args) ==
               {command |> String.downcase() |> String.to_atom(),
                {:error, "ERR timeout is not a float or out of range"}}
    end
  end

  test "Flow special grammar is case-insensitive and counted operands are exact" do
    assert ast("FLOW.CREATE", [
             "flow-1",
             "type",
             "job",
             "attribute",
             "tenant",
             "acme"
           ]) ==
             {:flow_create, "flow-1", [attributes: %{"tenant" => "acme"}, type: "job"]}

    assert ast("FLOW.SIGNAL", [
             "flow-1",
             "value",
             "result",
             "ok",
             "drop_value",
             "temporary"
           ]) ==
             {:flow_signal, "flow-1", drop_values: ["temporary"], values: [{"result", "ok"}]}

    assert ast("FLOW.SEARCH", ["attribute", "tenant", "acme"]) ==
             {:flow_search, attributes: %{"tenant" => "acme"}}

    assert ast("FLOW.CLAIM_DUE", [
             "job",
             "full",
             "false",
             "partitions",
             "2",
             "tenant-a",
             "tenant-b"
           ]) ==
             {:flow_claim_due, "job",
              [full: false, partition_keys: ["tenant-a", "tenant-b"]]}

    assert ast("FLOW.CLAIM_DUE", ["job", "partitions", "2", "tenant-a"]) ==
             {:flow_claim_due, "job", {:error, "ERR flow partition_keys count mismatch"}}

    assert ast("FLOW.POLICY.SET", ["job", "state", "queued", "mode", "fifo"]) ==
             {:flow_policy_set, "job", states: [{"queued", [mode: :fifo]}]}

    assert ast("FLOW.CREATE_MANY", ["tenant", "items", "flow-1", "payload"]) ==
             {:flow_create_many, "tenant", [{:id, "flow-1", :payload, "payload"}], []}

    assert ast("FLOW.SPAWN_CHILDREN", [
             "parent",
             "items",
             "mixed",
             "child",
             "tenant",
             "job",
             "payload"
           ]) ==
             {:flow_spawn_children, "parent",
             [{"child", [partition_key: "tenant", type: "job", payload: "payload"]}], []}
  end

  test "repeated Flow list options preserve order without append-based quadratic accumulation" do
    value_args = Enum.flat_map(1..1_000, fn index -> ["VALUE", "key-#{index}", "value"] end)

    assert {:flow_signal, "flow", opts} = ast("FLOW.SIGNAL", ["flow" | value_args])

    assert Keyword.fetch!(opts, :values) ==
             Enum.map(1..1_000, fn index -> {"key-#{index}", "value"} end)

    source = File.read!("lib/ferricstore/commands/native_ast_parser.ex")
    refute source =~ "&1 ++ [value]"
  end

  defp ast(command, args) do
    assert {:ok, ^command, _parsed_args, ast, _keys} = NativeAstParser.parse(command, args)
    ast
  end
end
