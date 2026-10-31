defmodule Ferricstore.Commands.StreamGroupInspectionTest do
  @moduledoc """
  XPENDING, XINFO GROUPS and XINFO CONSUMERS read a consumer group's pending
  entries and consumers, as Redis answers them.
  """
  use ExUnit.Case, async: false

  alias Ferricstore.Commands.Dispatcher

  setup do
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    ctx = FerricStore.Instance.get(:default)
    tag = "xinspect:{#{System.unique_integer([:positive])}}"
    d = fn command, args -> Dispatcher.dispatch(command, args, ctx) end
    k = &(tag <> &1)
    on_exit(fn -> for suffix <- ~w(s empty plain), do: FerricStore.del(k.(suffix)) end)

    :ok = d.("XGROUP", ["CREATE", k.("s"), "g", "0", "MKSTREAM"])
    for id <- ~w(1-0 2-0 3-0), do: ^id = d.("XADD", [k.("s"), id, "f", "v"])
    [[_, [_, _]]] = d.("XREADGROUP", ~w(GROUP g alice COUNT 2 STREAMS) ++ [k.("s"), ">"])
    [[_, [_]]] = d.("XREADGROUP", ~w(GROUP g bob COUNT 1 STREAMS) ++ [k.("s"), ">"])
    %{d: d, k: k}
  end

  test "XPENDING summarizes a group's pending entries", %{d: d, k: k} do
    assert [3, "1-0", "3-0", [["alice", "2"], ["bob", "1"]]] == d.("XPENDING", [k.("s"), "g"])

    assert 1 == d.("XACK", [k.("s"), "g", "1-0"])
    assert [2, "2-0", "3-0", [["alice", "1"], ["bob", "1"]]] == d.("XPENDING", [k.("s"), "g"])

    :ok = d.("XGROUP", ["CREATE", k.("empty"), "g", "0", "MKSTREAM"])
    assert [0, nil, nil, nil] == d.("XPENDING", [k.("empty"), "g"])
  end

  test "XPENDING lists entries in a range", %{d: d, k: k} do
    assert [["1-0", "alice", idle, 1], ["2-0", "alice", _, 1], ["3-0", "bob", _, 1]] =
             d.("XPENDING", [k.("s"), "g", "-", "+", "10"])

    assert is_integer(idle) and idle >= 0

    assert [["3-0", "bob", _, 1]] = d.("XPENDING", [k.("s"), "g", "-", "+", "10", "bob"])
    assert [["2-0", "alice", _, 1]] = d.("XPENDING", [k.("s"), "g", "(1-0", "+", "1"])
    assert [["1-0" | _], ["2-0" | _]] = d.("XPENDING", [k.("s"), "g", "-", "(3-0", "10"])
    assert [] == d.("XPENDING", [k.("s"), "g", "IDLE", "100000", "-", "+", "10"])
    assert 3 == length(d.("XPENDING", [k.("s"), "g", "IDLE", "0", "-", "+", "10"]))
  end

  test "XPENDING errors", %{d: d, k: k} do
    assert {:error, "NOGROUP No such key '#{k.("s")}' or consumer group 'nope'"} ==
             d.("XPENDING", [k.("s"), "nope"])

    assert {:error, "NOGROUP No such key '#{k.("missing")}' or consumer group 'g'"} ==
             d.("XPENDING", [k.("missing"), "g"])

    :ok = d.("SET", [k.("plain"), "x"])
    assert {:error, "WRONGTYPE" <> _} = d.("XPENDING", [k.("plain"), "g"])
    assert {:error, "ERR syntax error"} = d.("XPENDING", [k.("s"), "g", "-", "+"])

    assert {:error, "ERR value is not an integer" <> _} =
             d.("XPENDING", [k.("s"), "g", "-", "+", "x"])

    assert {:error, "ERR Invalid stream ID" <> _} = d.("XPENDING", [k.("s"), "g", "a", "+", "1"])
  end

  test "XINFO GROUPS and XINFO CONSUMERS", %{d: d, k: k} do
    :ok = d.("XGROUP", ["CREATE", k.("s"), "idle", "$"])

    assert [
             %{"name" => "g", "consumers" => 2, "pending" => 3, "last-delivered-id" => "3-0"},
             %{"name" => "idle", "consumers" => 0, "pending" => 0, "last-delivered-id" => "3-0"}
           ] = d.("XINFO", ["GROUPS", k.("s")])

    assert [
             %{"name" => "alice", "pending" => 2, "idle" => idle, "inactive" => _},
             %{"name" => "bob", "pending" => 1}
           ] = d.("XINFO", ["CONSUMERS", k.("s"), "g"])

    assert is_integer(idle) and idle >= 0
    assert [] == d.("XINFO", ["CONSUMERS", k.("s"), "idle"])

    assert {:error, "NOGROUP No such consumer group 'nope' for key name '#{k.("s")}'"} ==
             d.("XINFO", ["CONSUMERS", k.("s"), "nope"])

    assert {:error, "ERR no such key"} == d.("XINFO", ["GROUPS", k.("missing")])
  end
end
