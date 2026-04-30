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

  test "command time guard ignores comments and docs" do
    source = """
    defmodule Example do
      # HLC.now_ms() in a comment should not fail the guard.
      @moduledoc "Ferricstore.HLC.now_ms() in docs should not fail the guard."
      def ok, do: :ok
    end
    """

    assert direct_hlc_now_ms_calls("lib/ferricstore/commands/example.ex", source) == []
  end

  test "command time guard catches AST calls" do
    source = """
    defmodule Example do
      def bad, do: HLC.now_ms()
      def also_bad, do: Ferricstore.HLC.now_ms()
    end
    """

    assert [
             {"lib/ferricstore/commands/example.ex", 2, "def bad, do: HLC.now_ms()"},
             {"lib/ferricstore/commands/example.ex", 3,
              "def also_bad, do: Ferricstore.HLC.now_ms()"}
           ] = direct_hlc_now_ms_calls("lib/ferricstore/commands/example.ex", source)
  end

  test "raft apply time guard catches raw system clock reads" do
    source = """
    defmodule Example do
      def os_time, do: System.os_time(:millisecond)
      def system_time, do: System.system_time(:millisecond)
      def erlang_system_time, do: :erlang.system_time(:millisecond)
      def seconds_are_not_apply_expiry_ms, do: System.system_time(:second)
    end
    """

    assert [
             {"lib/ferricstore/raft/example.ex", 2,
              "def os_time, do: System.os_time(:millisecond)"},
             {"lib/ferricstore/raft/example.ex", 3,
              "def system_time, do: System.system_time(:millisecond)"},
             {"lib/ferricstore/raft/example.ex", 4,
              "def erlang_system_time, do: :erlang.system_time(:millisecond)"}
           ] = raw_clock_calls("lib/ferricstore/raft/example.ex", source)
  end

  test "raft apply path uses CommandTime instead of raw clock reads" do
    # StateMachine.apply/3 is replayed on every replica. A direct HLC or system
    # clock read here would make apply timing depend on the follower's local
    # execution time. The only allowed source is CommandTime, which exposes the
    # stamped log-entry time installed by the wrapper command.
    path = Path.join(@root, "lib/ferricstore/raft/state_machine.ex")

    assert raw_clock_calls(path) == []
  end

  defp direct_hlc_now_ms_calls(path) do
    path
    |> File.read!()
    |> direct_hlc_now_ms_calls(Path.relative_to(path, @root))
  end

  defp direct_hlc_now_ms_calls(relative, source) do
    ast_clock_calls(source, relative, [
      {{:aliases, [:HLC]}, :now_ms, 0},
      {{:aliases, [:Ferricstore, :HLC]}, :now_ms, 0}
    ])
  end

  defp raw_clock_calls(path) do
    relative = Path.relative_to(path, @root)

    path
    |> File.read!()
    |> raw_clock_calls(relative)
  end

  defp raw_clock_calls(relative, source) do
    source
    |> ast_clock_calls(relative, [
      {{:aliases, [:HLC]}, :now_ms, 0},
      {{:aliases, [:Ferricstore, :HLC]}, :now_ms, 0},
      {{:aliases, [:System]}, :os_time, 1, [:millisecond]},
      {{:aliases, [:System]}, :system_time, 1, [:millisecond]},
      {{:atom, :erlang}, :system_time, 1, [:millisecond]}
    ])
  end

  defp ast_clock_calls(source, relative, specs) do
    lines = String.split(source, "\n")
    {:ok, ast} = Code.string_to_quoted(source, columns: true)

    {_ast, violations} =
      Macro.prewalk(ast, [], fn
        {{:., meta, [target, fun]}, _call_meta, args} = node, acc when is_list(args) ->
          arity = length(args)

          if forbidden_call?(target, fun, arity, args, specs) do
            line_no = Keyword.get(meta, :line, 1)
            {node, [{relative, line_no, line_at(lines, line_no)} | acc]}
          else
            {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(violations)
  end

  defp forbidden_call?(target, fun, arity, args, specs) do
    Enum.any?(specs, fn
      {target_spec, ^fun, ^arity} ->
        target_matches?(target, target_spec)

      {target_spec, ^fun, ^arity, expected_args} ->
        target_matches?(target, target_spec) and args_match?(args, expected_args)

      _ ->
        false
    end)
  end

  defp target_matches?({:__aliases__, _meta, aliases}, {:aliases, aliases}), do: true
  defp target_matches?(atom, {:atom, atom}) when is_atom(atom), do: true
  defp target_matches?(_target, _spec), do: false

  defp args_match?(args, expected_args) do
    Enum.map(args, &literal_arg/1) == expected_args
  end

  defp literal_arg({atom, _meta, context}) when is_atom(atom) and is_atom(context), do: atom
  defp literal_arg(other), do: other

  defp line_at(lines, line_no) do
    lines
    |> Enum.at(line_no - 1, "")
    |> String.trim()
  end
end
