defmodule Ferricstore.Commands.StreamArgsTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Stream.Args

  test "XREAD rejects invalid duplicate and unknown options before STREAMS" do
    for args <- [
          ["COUNT", "bad", "STREAMS", "stream", "0"],
          ["COUNT", "-1", "STREAMS", "stream", "0"],
          ["BLOCK", "bad", "STREAMS", "stream", "0"],
          ["COUNT", "1", "COUNT", "2", "STREAMS", "stream", "0"],
          ["BOGUS", "value", "STREAMS", "stream", "0"]
        ] do
      assert {:error, _message} = Args.parse_xread_args(args)
    end
  end

  test "XREADGROUP rejects invalid duplicate and unknown options before STREAMS" do
    for options <- [
          ["COUNT", "bad"],
          ["BLOCK", "-1"],
          ["BLOCK", "1", "BLOCK", "2"],
          ["BOGUS", "value"]
        ] do
      args = ["GROUP", "group", "consumer" | options ++ ["STREAMS", "stream", ">"]]
      assert {:error, _message} = Args.parse_xreadgroup_args(args)
    end
  end
end
