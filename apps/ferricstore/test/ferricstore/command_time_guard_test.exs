defmodule Ferricstore.CommandTimeGuardTest do
  use ExUnit.Case, async: true

  @root Path.expand("../..", __DIR__)

  test "command modules use CommandTime instead of reading HLC time directly" do
    # Command modules can run inside Raft apply through Dispatcher.dispatch/3.
    # Relative expiry, absolute-expiry comparisons, TTL reporting, stream IDs,
    # and similar time-derived outputs must use the timestamp carried by the
    # Raft log entry. Direct HLC.now_ms/0 here would let each replica use its
    # local apply time and create small but real state divergence.
    violations =
      @root
      |> Path.join("lib/ferricstore/commands/**/*.ex")
      |> Path.wildcard()
      |> Enum.flat_map(&direct_hlc_now_ms_calls/1)

    assert violations == []
  end

  defp direct_hlc_now_ms_calls(path) do
    relative = Path.relative_to(path, @root)

    path
    |> File.read!()
    |> String.split("\n")
    |> Enum.with_index(1)
    |> Enum.flat_map(fn {line, line_no} ->
      if String.contains?(line, ["HLC.now_ms()", "Ferricstore.HLC.now_ms()"]) do
        [{relative, line_no, String.trim(line)}]
      else
        []
      end
    end)
  end
end
