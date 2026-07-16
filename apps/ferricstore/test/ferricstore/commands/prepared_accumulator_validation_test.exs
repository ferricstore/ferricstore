defmodule Ferricstore.Commands.PreparedAccumulatorValidationTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.PreparedAccumulatorCommand

  test "malformed typed transaction commands fail closed without raising" do
    invalid_commands = [
      {:set, "key", "value", %{ttl: 1}},
      {:set, "key", "value", [unknown: true]},
      {:set, "key", "value", [px: "100"]},
      {:incr_by, "key", "1"},
      {:hset, "key", [:invalid_pair]},
      {:lpush, "key", :not_a_list},
      {:rpush, "key", [1]},
      {:sadd, "key", [1]},
      {:zadd, "key", [{:invalid_pair}]},
      {:expire, "key", "100"}
    ]

    for command <- invalid_commands do
      assert {:error, "ERR invalid transaction command"} ==
               PreparedAccumulatorCommand.prepare(command)
    end

    assert {:error, "ERR invalid transaction command"} ==
             PreparedAccumulatorCommand.prepare_all(:not_a_list)
  end
end
