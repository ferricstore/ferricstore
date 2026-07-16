defmodule Ferricstore.Commands.StreamArgumentLimitsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Stream.Args

  test "stream arguments normalize case-sensitive protocol keywords" do
    assert {:ok, "stream", :auto, ["field", "value"], nil, true} =
             Args.parse_xadd_args(["stream", "nomkstream", "*", "field", "value"])

    assert {:ok, "stream", :auto, ["field", "value"], {:maxlen, false, 1}, false} =
             Args.parse_xadd_args(["stream", "maxlen", "1", "*", "field", "value"])

    assert {:ok, "group", "consumer", :infinity, :no_block, [{"stream", ">"}]} =
             Args.parse_xreadgroup_args(["group", "group", "consumer", "streams", "stream", ">"])
  end

  test "range COUNT rejects trailing arguments" do
    assert {:error, "ERR syntax error"} = Args.parse_count_opt(["COUNT", "1", "trailing"])
  end

  test "blocking stream reads reject timeouts above the VM timer ceiling" do
    too_large = Integer.to_string(0xFFFFFFFF + 1)

    assert {:error, "ERR value is not an integer or out of range"} =
             Args.parse_xread_args(["BLOCK", too_large, "STREAMS", "stream", "0"])

    assert {:error, "ERR value is not an integer or out of range"} =
             Args.parse_xreadgroup_args([
               "GROUP",
               "group",
               "consumer",
               "BLOCK",
               too_large,
               "STREAMS",
               "stream",
               ">"
             ])
  end
end
