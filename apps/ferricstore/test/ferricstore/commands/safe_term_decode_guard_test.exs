defmodule Ferricstore.Commands.SafeTermDecodeGuardTest do
  use ExUnit.Case, async: true

  @root Path.expand("../../..", __DIR__)

  @guarded_files [
    "lib/ferricstore.ex",
    "lib/ferricstore/commands/bloom.ex",
    "lib/ferricstore/commands/prob_type.ex",
    "lib/ferricstore/commands/stream.ex",
    "lib/ferricstore/commands/strings.ex",
    "lib/ferricstore/commands/tdigest.ex",
    "lib/ferricstore/merge/manifest.ex",
    "lib/ferricstore/raft/state_machine.ex",
    "lib/ferricstore/store/list_ops.ex"
  ]

  test "command-facing persisted term decoders use binary_to_term safe mode" do
    violations =
      @guarded_files
      |> Enum.flat_map(&unsafe_binary_to_term_calls/1)
      |> Enum.sort()

    assert violations == [],
           "Use :erlang.binary_to_term(value, [:safe]) for persisted command data:\n" <>
             Enum.map_join(violations, "\n", fn {file, line, code} ->
               "#{file}:#{line}: #{code}"
             end)
  end

  defp unsafe_binary_to_term_calls(relative_path) do
    path = Path.join(@root, relative_path)
    source = File.read!(path)
    lines = String.split(source, "\n")
    {:ok, ast} = Code.string_to_quoted(source, token_metadata: true)

    {_ast, violations} =
      Macro.prewalk(ast, [], fn
        {{:., meta, [:erlang, :binary_to_term]}, _call_meta, args} = node, acc ->
          if safe_binary_to_term_call?(args) do
            {node, acc}
          else
            line = meta[:line] || 0
            code = Enum.at(lines, max(line - 1, 0), "") |> String.trim()
            {node, [{relative_path, line, code} | acc]}
          end

        node, acc ->
          {node, acc}
      end)

    violations
  end

  defp safe_binary_to_term_call?([_value, [:safe]]), do: true
  defp safe_binary_to_term_call?([[:safe]]), do: true
  defp safe_binary_to_term_call?(_args), do: false
end
