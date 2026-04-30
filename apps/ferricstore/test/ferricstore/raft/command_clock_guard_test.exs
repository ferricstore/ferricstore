defmodule Ferricstore.Raft.CommandClockGuardTest do
  use ExUnit.Case, async: true

  @root Path.expand("../../..", __DIR__)

  @allowed_direct_ra_calls %{
    "lib/ferricstore/raft/command_clock.ex" => [
      ":ra.process_command(shard_id, stamp(command))",
      ":ra.process_command(shard_id, stamp(command), opts)",
      ":ra.pipeline_command(shard_id, stamp(command), corr, priority)"
    ],
    "lib/ferricstore/raft/batcher.ex" => [
      ":ra.pipeline_command(state.shard_id, serialized, corr, :normal)",
      ":ra.pipeline_command(target, serialized, corr, :normal)"
    ]
  }

  test "production raft submissions go through CommandClock or pre-stamped Batcher payloads" do
    # StateMachine.apply/3 must make TTL/lock decisions from a timestamp stored
    # in the Raft log, not from follower-local time. CommandClock stamps that
    # timestamp before commands enter Raft. This guard blocks future direct
    # production submits that would bypass HLC stamping and reintroduce
    # non-deterministic apply behavior across replicas.
    violations =
      production_files()
      |> Enum.flat_map(&direct_ra_calls/1)
      |> Enum.reject(&allowed_call?/1)

    assert violations == []
  end

  defp production_files do
    @root
    |> Path.join("lib/ferricstore/**/*.ex")
    |> Path.wildcard()
    |> Enum.reject(&String.ends_with?(&1, "/raft/command_clock_guard_test.exs"))
  end

  defp direct_ra_calls(path) do
    relative = Path.relative_to(path, @root)

    path
    |> File.read!()
    |> String.split("\n")
    |> Enum.with_index(1)
    |> Enum.flat_map(fn {line, line_no} ->
      if String.contains?(line, [":ra.process_command(", ":ra.pipeline_command("]) do
        [{relative, line_no, String.trim(line)}]
      else
        []
      end
    end)
  end

  defp allowed_call?({relative, _line_no, line}) do
    relative
    |> allowed_lines_for()
    |> Enum.any?(&String.contains?(line, &1))
  end

  defp allowed_lines_for(relative), do: Map.get(@allowed_direct_ra_calls, relative, [])
end
