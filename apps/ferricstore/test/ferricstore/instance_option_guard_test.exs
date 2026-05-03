defmodule Ferricstore.InstanceOptionGuardTest do
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)

  test "production code does not expose the removed raft_enabled option" do
    matches =
      @root
      |> Path.join("{lib,config}/**/*.{ex,exs}")
      |> Path.wildcard()
      |> Enum.flat_map(fn path ->
        path
        |> File.read!()
        |> String.split("\n")
        |> Enum.with_index(1)
        |> Enum.flat_map(fn {line, line_no} ->
          if String.contains?(line, "raft_enabled") do
            ["#{Path.relative_to(path, @root)}:#{line_no}: #{line}"]
          else
            []
          end
        end)
      end)

    assert matches == [],
           "raft_enabled was removed; production code should not keep compatibility hooks:\n" <>
             Enum.join(matches, "\n")
  end
end
