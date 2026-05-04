defmodule Ferricstore.Transaction.AstTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Transaction.Ast, as: TxAst

  test "test two-tuple queue entries are normalized through the Rust RESP parser" do
    assert {"SET", ["k", "v", "nx"], {:set, "k", "v", [:nx]}} =
             TxAst.normalize_entry({"set", ["k", "v", "nx"]})

    assert {:set, "k", "v", {:error, "ERR XX and NX options at the same time are not compatible"}} =
             TxAst.command_ast({"SET", ["k", "v", "NX", "XX"]})
  end

  test "namespaces multi-key AST shapes used by transaction execution" do
    ns = "sb:"

    assert {:mget, ["sb:a", "sb:b"]} = TxAst.namespace_first_key({:mget, ["a", "b"]}, ns)

    assert {:mset, ["sb:a", "1", "sb:b", "2"]} =
             TxAst.namespace_first_key({:mset, ["a", "1", "b", "2"]}, ns)

    assert {:bitop, :band, "sb:dst", ["sb:a", "sb:b"]} =
             TxAst.namespace_first_key({:bitop, :band, "dst", ["a", "b"]}, ns)

    assert {:json_mget, ["sb:a", "sb:b"], ["name"]} =
             TxAst.namespace_first_key({:json_mget, ["a", "b"], ["name"]}, ns)

    assert {:sintercard, ["sb:s1", "sb:s2"], 0} =
             TxAst.namespace_first_key({:sintercard, ["s1", "s2"], 0}, ns)
  end

  test "namespaces stream read keys without changing IDs or group metadata" do
    ns = "sb:"

    assert {:xread, 10, 0, [{"sb:s1", "0-0"}, {"sb:s2", "$"}]} =
             TxAst.namespace_first_key({:xread, 10, 0, [{"s1", "0-0"}, {"s2", "$"}]}, ns)

    assert {:xreadgroup, "g", "c", {5, 0, [{"sb:s1", ">"}]}} =
             TxAst.namespace_first_key({:xreadgroup, "g", "c", {5, 0, [{"s1", ">"}]}}, ns)
  end

  test "does not namespace command names, subcommands, or malformed AST payloads" do
    ns = "sb:"

    assert {:unknown, "NOPE", ["k"]} =
             TxAst.namespace_first_key({:unknown, "NOPE", ["k"]}, ns)

    assert {:acl, "SETUSER", ["u"]} =
             TxAst.namespace_first_key({:acl, "SETUSER", ["u"]}, ns)

    assert {:object, :encoding, "sb:k"} =
             TxAst.namespace_first_key({:object, :encoding, "k"}, ns)

    assert {:object, {:error, "ERR bad"}} =
             TxAst.namespace_first_key({:object, {:error, "ERR bad"}}, ns)
  end
end
