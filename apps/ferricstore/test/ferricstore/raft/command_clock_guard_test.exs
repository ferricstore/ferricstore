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
      ":ra.pipeline_command(target, serialized, corr, priority)"
    ]
  }

  test "production raft submissions go through CommandClock or pre-stamped Batcher payloads" do
    # StateMachine.apply/3 must make TTL/lock decisions from a timestamp stored
    # in the Raft log, not from follower-local time. CommandClock stamps that
    # timestamp before commands enter Raft. This guard blocks future direct
    # production submits that would bypass HLC stamping and reintroduce
    # non-deterministic apply behavior across replicas.
    #
    # Batcher is the one allowed direct pipeline path because it serializes
    # commands with CommandClock.to_ttb/1 first; the raw Ra call must stay
    # behind that single wrapper so target liveness/reply handling is uniform.
    violations =
      production_files()
      |> Enum.flat_map(&direct_ra_calls/1)
      |> Enum.reject(&allowed_call?/1)

    assert violations == []
  end

  test "direct raft scanner ignores comments and detects executable calls" do
    source = """
    defmodule Example do
      # :ra.process_command(commented, :ignored)

      def run(target) do
        :ra.pipeline_command(target, :cmd, make_ref(), :normal)
      end
    end
    """

    assert [
             {"lib/ferricstore/example.ex", 5,
              ":ra.pipeline_command(target, :cmd, make_ref(), :normal)"}
           ] =
             direct_ra_calls_in_source("lib/ferricstore/example.ex", source)
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
    |> then(&direct_ra_calls_in_source(relative, &1))
  end

  defp direct_ra_calls_in_source(relative, source) do
    lines =
      source
      |> String.split("\n")
      |> Enum.with_index(1)
      |> Map.new(fn {line, line_no} -> {line_no, String.trim(line)} end)

    {:ok, ast} = Code.string_to_quoted(source)

    {_ast, calls} =
      Macro.prewalk(ast, [], fn
        {{:., meta, [:ra, fun]}, _call_meta, args} = node, acc
        when fun in [:process_command, :pipeline_command] and is_list(args) ->
          line_no = Keyword.fetch!(meta, :line)
          {node, [{relative, line_no, Map.fetch!(lines, line_no)} | acc]}

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(calls)
  end

  defp allowed_call?({relative, _line_no, line}) do
    relative
    |> allowed_lines_for()
    |> Enum.any?(&String.contains?(line, &1))
  end

  defp allowed_lines_for(relative), do: Map.get(@allowed_direct_ra_calls, relative, [])
end
