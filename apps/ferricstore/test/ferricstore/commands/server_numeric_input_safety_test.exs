defmodule Ferricstore.Commands.ServerNumericInputSafetyTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Commands.Server

  test "DEBUG SLEEP rejects timeouts outside the VM timer range" do
    huge_seconds = String.duplicate("9", 1_000)

    task =
      Task.async(fn ->
        Server.handle("DEBUG", ["SLEEP", huge_seconds], %{})
      end)

    result =
      case Task.yield(task, 100) || Task.shutdown(task, :brutal_kill) do
        {:ok, value} -> value
        nil -> :blocked
      end

    assert {:error, "ERR invalid argument for DEBUG SLEEP"} ==
             result
  end
end
