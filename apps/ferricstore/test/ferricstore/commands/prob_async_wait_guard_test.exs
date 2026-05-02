defmodule Ferricstore.Commands.ProbAsyncWaitGuardTest do
  use ExUnit.Case, async: true

  @command_files ~w[
    bloom.ex
    cms.ex
    cuckoo.ex
    topk.ex
  ]
                 |> Enum.map(&Path.expand("../../../lib/ferricstore/commands/#{&1}", __DIR__))

  test "probabilistic read commands do not wait on async NIF replies in caller mailbox" do
    violations =
      @command_files
      |> Enum.flat_map(&direct_self_async_nif_calls/1)

    assert violations == []
  end

  defp direct_self_async_nif_calls(path) do
    ast =
      path
      |> File.read!()
      |> Code.string_to_quoted!(columns: true)

    {_ast, violations} =
      Macro.prewalk(ast, [], fn
        {{:., meta, [mod, fun]}, _call_meta, [first_arg | _]} = node, acc
        when is_atom(fun) ->
          if async_nif_module?(mod) and async_fun?(fun) and self_call?(first_arg) do
            line = Keyword.get(meta, :line, 1)
            {node, ["#{path}:#{line}: #{fun}/#{length(elem(node, 2))}" | acc]}
          else
            {node, acc}
          end

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(violations)
  end

  defp async_nif_module?({:__aliases__, _, [:NIF]}), do: true
  defp async_nif_module?({:__aliases__, _, [:Ferricstore, :Bitcask, :NIF]}), do: true
  defp async_nif_module?(_), do: false

  defp async_fun?(fun), do: fun |> Atom.to_string() |> String.ends_with?("_async")

  defp self_call?({:self, _, []}), do: true
  defp self_call?(_), do: false
end
